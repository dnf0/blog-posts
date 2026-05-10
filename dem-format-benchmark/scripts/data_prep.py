"""Stage 1: Download Copernicus GLO-30 DEM COG, smooth, and convert to all target formats.

Generates 4 data variants (raw, s3, s15, s21) for each of 5 formats (COG,
Parquet flat, Parquet+S2, Parquet+H3, GeoParquet). Parquet files use Hilbert
curve ordering within chunks for RLE/dictionary compression efficiency.

All processing is streaming (batch-based) to stay within 8 GB RAM.
"""

import json
import time
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import rasterio
from rasterio.windows import Window
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    DATA_DIR, DATA_VARIANTS, REGION_BOUNDS, S2_LEVEL, H3_RESOLUTION,
    SMOOTHING_SIGMAS, FORMAT_PATHS, FORMAT_LABELS, TABLES_DIR,
)


# ======================================================================
# Hilbert curve (vectorized, order p, 2D)
# ======================================================================

def _hilbert_indices(x_arr, y_arr, x_min, y_min, x_scale, y_scale, order=15):
    """Compute Hilbert curve indices for arrays of (x, y) coordinates.

    Uses the classic bit-manipulation algorithm vectorized with numpy.
    Returns int64 indices suitable for sorting.
    """
    n = 1 << order
    xi = ((x_arr - x_min) / x_scale).astype(np.int64)
    yi = ((y_arr - y_min) / y_scale).astype(np.int64)
    xi = np.clip(xi, 0, n - 1)
    yi = np.clip(yi, 0, n - 1)

    # Vectorized Hilbert curve algorithm
    d = np.zeros(len(xi), dtype=np.int64)
    s = n >> 1
    while s > 0:
        rx = (xi & s) > 0
        ry = (yi & s) > 0
        d += np.int64(s) * s * ((3 * rx.astype(np.int64)) ^ ry.astype(np.int64))
        # Rotate quadrants
        mask_no_rx = ~rx
        mask_rx_ry = rx & ~ry
        mask_rx_nry = rx & ry
        # When rx=0, ry=1: swap x and y
        xi_temp = xi.copy()
        xi = np.where(mask_no_rx & ry, yi, xi)
        yi = np.where(mask_no_rx & ry, xi_temp, yi)
        # When rx=1, ry=0: rotate to bottom-right
        xi = np.where(mask_rx_ry, n - 1 - xi, xi)
        yi = np.where(mask_rx_ry, n - 1 - yi, yi)
        # When rx=1, ry=1: rotate to top-right
        xi = np.where(mask_rx_nry, n - 1 - xi, xi)
        yi = np.where(mask_rx_nry, n - 1 - yi, yi)
        s >>= 1
    return d


def sort_by_hilbert(df, x_col="x", y_col="y", order=15):
    """Sort a DataFrame by Hilbert curve index for spatial locality."""
    x = df[x_col].to_numpy()
    y = df[y_col].to_numpy()
    res = 30.0 / 111320.0  # ~30m at these latitudes
    hilbert = _hilbert_indices(
        x, y, REGION_BOUNDS["min_lon"], REGION_BOUNDS["min_lat"], res, res, order,
    )
    order_idx = np.argsort(hilbert)
    return df.iloc[order_idx].reset_index(drop=True)


def _arrays_to_arrow_table(columns):
    """Build an Arrow table from NumPy arrays using the benchmark schema types."""
    import pyarrow as pa

    arrow_columns = {}
    for name, values in columns.items():
        if name in ("x", "y"):
            arrow_columns[name] = pa.array(values, pa.float64())
        elif name == "band_value":
            arrow_columns[name] = pa.array(values, pa.int32())
        elif name == "s2_cell":
            arrow_columns[name] = pa.array(values, pa.int64())
        elif name == "h3_cell":
            arrow_columns[name] = pa.array(values, pa.uint64())
        else:
            arrow_columns[name] = pa.array(values)
    return pa.table(arrow_columns)


def _cell_table_sorted(x_col, y_col, val_col, cell_col, cell_name):
    order_idx = np.argsort(cell_col)
    columns = {
        "x": x_col[order_idx],
        "y": y_col[order_idx],
        "band_value": val_col[order_idx],
        cell_name: cell_col[order_idx],
    }
    return _arrays_to_arrow_table(columns)


def _take_arrow_table(table, order_idx):
    """Return a table with every column reordered by a NumPy integer index."""
    import pyarrow as pa

    take_idx = pa.array(order_idx.astype(np.int64, copy=False), pa.int64())
    return table.take(take_idx)


def _quantize_band_values(values):
    return np.round(values.astype(np.float32, copy=False) * 10).astype(np.int32)


def _valid_data_mask(data, nodata):
    valid_mask = np.isfinite(data)
    if nodata is not None and np.isfinite(nodata):
        valid_mask &= data != nodata
    return valid_mask


def _pixel_centers_for_valid_window(transform, window, valid_mask):
    rows, cols = np.nonzero(valid_mask)
    x = transform.c + (cols + window.col_off + 0.5) * transform.a
    y = transform.f + (rows + window.row_off + 0.5) * transform.e
    return x.astype(np.float64, copy=False), y.astype(np.float64, copy=False), rows, cols


def _table_sorted_by_hilbert(table, order=15):
    x = table.column("x").to_numpy()
    y = table.column("y").to_numpy()
    res = 30.0 / 111320.0
    hilbert = _hilbert_indices(
        x, y, REGION_BOUNDS["min_lon"], REGION_BOUNDS["min_lat"], res, res, order,
    )
    return _take_arrow_table(table, np.argsort(hilbert))


def _iter_cog_arrow_batches(cog_path, strip_size=256, sort_hilbert=True):
    """Yield Arrow tables of valid COG pixels without per-pixel Python loops."""
    with rasterio.open(cog_path) as src:
        width, height = src.width, src.height
        transform = src.transform
        for y_start in range(0, height, strip_size):
            y_end = min(y_start + strip_size, height)
            window = Window(0, y_start, width, y_end - y_start)
            data = src.read(1, window=window)
            valid_mask = _valid_data_mask(data, src.nodata)
            if not valid_mask.any():
                continue

            x, y, rows, cols = _pixel_centers_for_valid_window(transform, window, valid_mask)
            table = _arrays_to_arrow_table({
                "x": x,
                "y": y,
                "band_value": _quantize_band_values(data[rows, cols]),
            })
            yield _table_sorted_by_hilbert(table) if sort_hilbert else table


def _sample_band_values_for_xy(src, x_arr, y_arr):
    """Sample quantized DEM values for existing x/y arrays from an open raster."""
    transform = src.transform
    pixel_width = transform.a
    pixel_height = abs(transform.e)
    origin_x = transform.c
    origin_y = transform.f
    col = ((x_arr - origin_x) / pixel_width).astype(np.int64)
    row = ((origin_y - y_arr) / pixel_height).astype(np.int64)
    col = np.clip(col, 0, src.width - 1)
    row = np.clip(row, 0, src.height - 1)
    values = np.empty(len(row), dtype=np.float32)
    for src_row in np.unique(row):
        positions = np.flatnonzero(row == src_row)
        data_row = src.read(1, window=Window(0, int(src_row), src.width, 1))[0]
        values[positions] = data_row[col[positions]]
    return _quantize_band_values(values)


def _iter_indexed_variant_batches(raw_pf, cog_path, cell_name, batch_size=500_000):
    """Yield raw-indexed x/y/cell rows with band values sampled from a variant COG."""
    with rasterio.open(cog_path) as src:
        for raw_batch in raw_pf.iter_batches(batch_size=batch_size):
            x_arr = raw_batch.column("x").to_numpy()
            y_arr = raw_batch.column("y").to_numpy()
            band_values = _sample_band_values_for_xy(src, x_arr, y_arr)
            yield _arrays_to_arrow_table({
                "x": x_arr,
                "y": y_arr,
                "band_value": band_values,
                cell_name: raw_batch.column(cell_name).to_numpy(),
            })


# ======================================================================
# Download
# ======================================================================

def _collect_tile_grid(bounds):
    min_lon = int(bounds["min_lon"])
    max_lon = int(bounds["max_lon"])
    min_lat = int(bounds["min_lat"])
    max_lat = int(bounds["max_lat"])
    tiles = []
    for lat in range(min_lat, max_lat):
        lat_char = "N" if lat >= 0 else "S"
        for lon in range(min_lon, max_lon):
            lon_char = "E" if lon >= 0 else "W"
            tile_name = (
                f"Copernicus_DSM_COG_10_{lat_char}{abs(lat):02d}_00"
                f"_{lon_char}{abs(lon):03d}_00_DEM"
            )
            url = f"/vsis3/copernicus-dem-30m/{tile_name}/{tile_name}.tif"
            tiles.append((lon, lat, url))
    return tiles


def download_copernicus_dem():
    cog_raw = FORMAT_PATHS["cog"]["raw"]
    if cog_raw.exists():
        print(f"COG already exists at {cog_raw}, skipping download")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    bounds = REGION_BOUNDS
    tiles = _collect_tile_grid(bounds)
    print(f"Building COG from {len(tiles)} Copernicus tiles via S3...")

    with rasterio.Env(AWS_NO_SIGN_REQUEST="YES"):
        with rasterio.open(tiles[0][2]) as src:
            tile_h, tile_w = src.height, src.width
            dtype = src.dtypes[0]

        lon_cols = int((bounds["max_lon"] - bounds["min_lon"]) / 1.0)
        lat_rows = int((bounds["max_lat"] - bounds["min_lat"]) / 1.0)
        out_w = lon_cols * tile_w
        out_h = lat_rows * tile_h

        out_transform = rasterio.transform.from_bounds(
            bounds["min_lon"], bounds["min_lat"],
            bounds["max_lon"], bounds["max_lat"],
            out_w, out_h,
        )

        profile = dict(
            driver="COG", height=out_h, width=out_w, count=1,
            dtype=dtype, crs="EPSG:4326", transform=out_transform,
            blockxsize=512, blockysize=512, tiled=True,
            compress="deflate", predictor=2,
        )

        max_lat_grid = int(bounds["max_lat"])
        with rasterio.open(cog_raw, "w", **profile) as dst:
            for lon, lat, url in tiles:
                col = lon - int(bounds["min_lon"])
                row = max_lat_grid - 1 - lat
                with rasterio.open(url) as src:
                    data = src.read(1)
                dst.write(data, indexes=1, window=rasterio.windows.Window(
                    col * tile_w, row * tile_h, tile_w, tile_h,
                ))
                print(f"  lon={lon} lat={lat}: {data.shape}")


# ======================================================================
# Gaussian smoothing
# ======================================================================

def _smooth_strip(data, sigma):
    """Apply Gaussian filter to a 2D array, preserving NaN/no-data."""
    from scipy.ndimage import gaussian_filter
    valid_mask = np.isfinite(data)
    # Fill NaN with 0 for filtering, then restore
    filled = np.where(valid_mask, data, 0.0)
    smoothed = gaussian_filter(filled.astype(np.float64), sigma=sigma, mode="nearest")
    smoothed[~valid_mask] = np.nan
    return smoothed.astype(np.float32)


def build_smoothed_cogs():
    """Create smoothed COGs at sigma 3, 15, 21 from the raw COG."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    cog_raw = FORMAT_PATHS["cog"]["raw"]
    if not cog_raw.exists():
        raise FileNotFoundError(f"Raw COG not found: {cog_raw}")

    for sigma in SMOOTHING_SIGMAS:
        tag = f"s{sigma}"
        cog_out = FORMAT_PATHS["cog"][tag]
        if cog_out.exists():
            print(f"Smoothed COG s{sigma} already exists, skipping")
            continue

        print(f"Smoothing COG with sigma={sigma}...")
        with rasterio.open(cog_raw) as src:
            profile = src.profile.copy()

            with rasterio.open(cog_out, "w", **profile) as dst:
                strip_size = 256
                for y_start in tqdm(
                    range(0, src.height, strip_size),
                    desc=f"  s{sigma} rows",
                ):
                    y_end = min(y_start + strip_size, src.height)
                    window = Window(0, y_start, src.width, y_end - y_start)
                    data = src.read(1, window=window)
                    smoothed = _smooth_strip(data, sigma)
                    dst.write(smoothed, indexes=1, window=window)

        size_mb = cog_out.stat().st_size / (1024 * 1024)
        print(f"  Wrote {cog_out.name} ({size_mb:.1f} MB)")


# ======================================================================
# Run-length analysis
# ======================================================================

def analyze_run_lengths():
    """Analyze run-length characteristics of each COG variant (streaming)."""
    out_path = TABLES_DIR / "dem_stats.json"
    if out_path.exists():
        print(f"DEM stats already exist at {out_path}, skipping analysis")
        return

    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    stats = {}

    for variant in DATA_VARIANTS:
        cog_path = FORMAT_PATHS["cog"][variant]
        if not cog_path.exists():
            print(f"  WARNING: {variant} COG not found, skipping analysis")
            continue

        print(f"Analyzing run lengths for {variant}...")
        with rasterio.open(cog_path) as src:
            # Streaming accumulation
            n_valid = 0
            vmin = float("inf")
            vmax = float("-inf")
            vsum = 0.0
            vsum2 = 0.0

            total_diffs = 0
            sum_abs_diff = 0.0
            changes_raw = 0
            # Change counts per quantization level
            changes_q = {0: 0, 1: 0, 5: 0, 10: 0}
            # Track last value from previous strip for boundary diffs
            prev_strip_last = None

            strip_size = 256
            for y_start in range(0, src.height, strip_size):
                y_end = min(y_start + strip_size, src.height)
                window = Window(0, y_start, src.width, y_end - y_start)
                data = src.read(1, window=window)

                # Flatten in row-major order and filter valid
                flat = data.ravel()
                valid = flat[np.isfinite(flat)]
                if len(valid) == 0:
                    continue

                n_valid += len(valid)
                vmin = min(vmin, float(valid.min()))
                vmax = max(vmax, float(valid.max()))
                vsum += float(valid.sum())
                vsum2 += float((valid * valid).sum())

                # Within-strip diffs
                strip_diffs = np.abs(np.diff(valid))
                total_diffs += len(strip_diffs)
                sum_abs_diff += float(strip_diffs.sum())
                changes_raw += int(np.sum(strip_diffs > 0))

                for quant in [0, 1, 5, 10]:
                    if quant == 0:
                        q_valid = valid
                    else:
                        q_valid = np.round(valid / quant) * quant
                    q_diffs = np.abs(np.diff(q_valid))
                    changes_q[quant] += int(np.sum(q_diffs > 0))

                # Boundary diff to previous strip
                if prev_strip_last is not None and len(valid) > 0:
                    boundary_diff = abs(float(prev_strip_last) - float(valid[0]))
                    total_diffs += 1
                    sum_abs_diff += boundary_diff
                    if boundary_diff > 0:
                        changes_raw += 1
                    for quant in [0, 1, 5, 10]:
                        if quant == 0:
                            a, b = float(prev_strip_last), float(valid[0])
                        else:
                            a = round(float(prev_strip_last) / quant) * quant
                            b = round(float(valid[0]) / quant) * quant
                        if a != b:
                            changes_q[quant] += 1

                prev_strip_last = valid[-1]

            if n_valid == 0:
                continue

            mean = vsum / n_valid
            variance = max(0, vsum2 / n_valid - mean * mean)

            run_lengths = {}
            for quant in [0, 1, 5, 10]:
                qc = changes_q[quant]
                if qc > 0 and total_diffs > 0:
                    mean_run = n_valid / qc
                else:
                    mean_run = n_valid
                run_lengths[f"{quant}m"] = {
                    "changes": qc,
                    "change_pct": round(qc / total_diffs * 100, 1) if total_diffs > 0 else 0.0,
                    "mean_run_length": round(mean_run, 1),
                }

            stats[variant] = {
                "pixels": n_valid,
                "min_elevation": round(vmin, 1),
                "max_elevation": round(vmax, 1),
                "mean_elevation": round(mean, 1),
                "std_elevation": round(float(np.sqrt(variance)), 1),
                "mean_abs_diff": round(sum_abs_diff / total_diffs, 3) if total_diffs > 0 else 0.0,
                "pct_changes": round(changes_raw / total_diffs * 100, 1) if total_diffs > 0 else 0.0,
                "run_lengths": run_lengths,
            }
            print(f"  {variant}: {n_valid:,} pixels, mean={mean:.1f}m, "
                  f"mean_abs_diff={stats[variant]['mean_abs_diff']:.3f}m")

    out_path.write_text(json.dumps(stats, indent=2) + "\n")
    print(f"Saved run-length analysis to {out_path}")


# ======================================================================
# Parquet writers — streaming, Hilbert-sorted, delta-encoded
# ======================================================================

def _write_batches_to_parquet(batches_iter, output_path, schema_override=None):
    """Stream PyArrow tables to a Parquet file with delta encoding.

    Uses DELTA_BINARY_PACKED for integer columns (band_value as int32,
    s2_cell, h3_cell) for maximum compression on sorted data.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    started = time.time()
    writer = None
    try:
        for tbl in batches_iter:
            if writer is None:
                schema = schema_override or tbl.schema
                col_encoding = {}
                for field in schema:
                    if pa.types.is_integer(field.type):
                        col_encoding[field.name] = "DELTA_BINARY_PACKED"
                writer = pq.ParquetWriter(
                    str(output_path), schema,
                    compression="zstd",
                    compression_level=15,
                    use_dictionary=False,
                    column_encoding=col_encoding,
                    write_statistics=True,
                )
            writer.write_table(tbl)
    finally:
        if writer is not None:
            writer.close()
    if writer is None:
        raise ValueError("no batches to write")

    elapsed = time.time() - started
    row_count = pq.ParquetFile(str(output_path)).metadata.num_rows
    size_mb = output_path.stat().st_size / (1024 * 1024)
    return row_count, size_mb, elapsed


# ---------------------------------------------------------------------------
# Flat Parquet (Hilbert-sorted, x / y / band_value as int32 delta)
# ---------------------------------------------------------------------------

def _build_flat(variant: str):
    """Build flat Parquet: for non-raw variants, reuse x/y from raw and sample band_value from smoothed COG."""
    import pyarrow.parquet as pq

    output_path = FORMAT_PATHS["parquet_flat"][variant]
    if output_path.exists():
        print(f"  flat ({variant}): already exists, skipping")
        return

    if variant == "raw":
        cog_path = FORMAT_PATHS["cog"]["raw"]
        print(f"  flat (raw): extracting from {cog_path.name}...")

        def _batch_generator():
            yield from _iter_cog_arrow_batches(cog_path, strip_size=256, sort_hilbert=True)

        row_count, size_mb, elapsed = _write_batches_to_parquet(_batch_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")
    else:
        print(f"  flat ({variant}): reusing x/y from raw flat, sampling band_value from COG...")
        raw_flat_path = FORMAT_PATHS["parquet_flat"]["raw"]
        if not raw_flat_path.exists():
            _build_flat("raw")

        cog_path = FORMAT_PATHS["cog"][variant]
        raw_pf = pq.ParquetFile(str(raw_flat_path))

        def _sample_generator():
            with rasterio.open(cog_path) as src:
                for batch in raw_pf.iter_batches(batch_size=500_000):
                    x_arr = batch.column("x").to_numpy()
                    y_arr = batch.column("y").to_numpy()
                    band_values = _sample_band_values_for_xy(src, x_arr, y_arr)
                    yield _arrays_to_arrow_table({
                        "x": x_arr,
                        "y": y_arr,
                        "band_value": band_values,
                    })

        row_count, size_mb, elapsed = _write_batches_to_parquet(_sample_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# S2 Parquet (Hilbert-sorted by s2_cell, x / y / band_value / s2_cell)
# ---------------------------------------------------------------------------

def _build_s2(variant: str):
    """Build S2 Parquet: for non-raw variants, reuse s2_cell from raw variant."""
    import pyarrow.parquet as pq

    output_path = FORMAT_PATHS["parquet_s2"][variant]
    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  s2   ({variant}): already exists ({size_mb:.1f} MB), skipping")
        return

    if variant == "raw":
        flat_path = FORMAT_PATHS["parquet_flat"][variant]
        if not flat_path.exists():
            _build_flat(variant)

        # Compute s2 cells from scratch (only done once for raw)
        import s2cell as _s2
        print(f"  s2   ({variant}): computing S2 cells and sorting...")
        pf = pq.ParquetFile(str(flat_path))
        _cell = _s2.s2cell.lat_lon_to_cell_id
        batch_idx = 0
        processed = 0

        def _batch_generator():
            nonlocal batch_idx, processed
            for batch in pf.iter_batches(batch_size=500_000):
                x_col = batch.column("x").to_numpy()
                y_col = batch.column("y").to_numpy()
                val_col = batch.column("band_value").to_numpy()

                s2_cells = np.empty(len(x_col), dtype=np.int64)
                for i in range(len(x_col)):
                    s2_cells[i] = _cell(y_col[i], x_col[i], S2_LEVEL)

                tbl = _cell_table_sorted(
                    x_col,
                    y_col,
                    val_col.astype(np.int32, copy=False),
                    s2_cells,
                    "s2_cell",
                )
                processed += len(tbl)
                batch_idx += 1
                if batch_idx % 50 == 0:
                    print(f"    batch {batch_idx}: {processed:,} rows")
                yield tbl

        row_count, size_mb, elapsed = _write_batches_to_parquet(_batch_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")
    else:
        # Reuse s2_cell from raw variant — S2 cell depends only on (x,y), not elevation
        print(f"  s2   ({variant}): reusing s2_cell from raw, swapping band_value...")
        raw_s2_path = FORMAT_PATHS["parquet_s2"]["raw"]
        if not raw_s2_path.exists():
            _build_s2("raw")

        cog_path = FORMAT_PATHS["cog"][variant]
        raw_pf = pq.ParquetFile(str(raw_s2_path))

        def _reuse_generator():
            yield from _iter_indexed_variant_batches(raw_pf, cog_path, "s2_cell")

        row_count, size_mb, elapsed = _write_batches_to_parquet(_reuse_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# H3 Parquet (sorted by h3_cell, x / y / band_value / h3_cell as uint64)
# ---------------------------------------------------------------------------

def _build_h3(variant: str):
    """Build H3 Parquet: for non-raw variants, reuse h3_cell from raw variant."""
    import pyarrow.parquet as pq

    output_path = FORMAT_PATHS["parquet_h3"][variant]
    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  h3   ({variant}): already exists ({size_mb:.1f} MB), skipping")
        return

    if variant == "raw":
        flat_path = FORMAT_PATHS["parquet_flat"][variant]
        if not flat_path.exists():
            _build_flat(variant)

        import h3.api.numpy_int as h3_mod
        print(f"  h3   ({variant}): computing H3 cells and sorting...")
        pf = pq.ParquetFile(str(flat_path))
        batch_idx = 0
        processed = 0
        _f = h3_mod.latlng_to_cell

        def _batch_generator():
            nonlocal batch_idx, processed
            for batch in pf.iter_batches(batch_size=500_000):
                x_col = batch.column("x").to_numpy()
                y_col = batch.column("y").to_numpy()
                val_col = batch.column("band_value").to_numpy()

                cells = np.empty(len(x_col), dtype=np.uint64)
                for i in range(len(x_col)):
                    cells[i] = _f(y_col[i], x_col[i], H3_RESOLUTION)

                tbl = _cell_table_sorted(
                    x_col,
                    y_col,
                    val_col.astype(np.int32, copy=False),
                    cells,
                    "h3_cell",
                )
                processed += len(tbl)
                batch_idx += 1
                if batch_idx % 50 == 0:
                    print(f"    batch {batch_idx}: {processed:,} rows")
                yield tbl

        row_count, size_mb, elapsed = _write_batches_to_parquet(_batch_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")
    else:
        print(f"  h3   ({variant}): reusing h3_cell from raw, swapping band_value...")
        raw_h3_path = FORMAT_PATHS["parquet_h3"]["raw"]
        if not raw_h3_path.exists():
            _build_h3("raw")

        cog_path = FORMAT_PATHS["cog"][variant]
        raw_pf = pq.ParquetFile(str(raw_h3_path))

        def _reuse_generator():
            yield from _iter_indexed_variant_batches(raw_pf, cog_path, "h3_cell")

        row_count, size_mb, elapsed = _write_batches_to_parquet(_reuse_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")


# ---------------------------------------------------------------------------
# GeoParquet (Hilbert-sorted, geometry / band_value)
# ---------------------------------------------------------------------------

def _build_geoparquet(variant: str):
    """Build GeoParquet: for non-raw variants, reuse geometry from raw variant."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    output_path = FORMAT_PATHS["geoparquet"][variant]
    if output_path.exists():
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"  geo  ({variant}): already exists ({size_mb:.1f} MB), skipping")
        return

    if variant == "raw":
        from shapely import points as shapely_points
        import geopandas as gpd

        flat_path = FORMAT_PATHS["parquet_flat"]["raw"]
        if not flat_path.exists():
            _build_flat("raw")

        print(f"  geo  (raw): writing GeoParquet...")
        pf = pq.ParquetFile(str(flat_path))
        chunk_num = 0
        total = 0

        temp_dir = DATA_DIR / "_geo_chunks_raw"
        temp_dir.mkdir(parents=True, exist_ok=True)
        geo_chunk_files = []

        for batch in pf.iter_batches(batch_size=500_000):
            x_arr = batch.column("x").to_numpy()
            y_arr = batch.column("y").to_numpy()
            val_arr = batch.column("band_value").to_numpy()

            df_chunk = pd.DataFrame({
                "x": x_arr, "y": y_arr, "band_value": val_arr,
            })
            df_chunk = sort_by_hilbert(df_chunk)

            geometry = shapely_points(df_chunk["x"].values, df_chunk["y"].values)
            gdf_chunk = gpd.GeoDataFrame(
                {"band_value": df_chunk["band_value"].values},
                geometry=geometry, crs="EPSG:4326",
            )
            chunk_path = temp_dir / f"chunk_{chunk_num:06d}.parquet"
            gdf_chunk.to_parquet(chunk_path, compression="zstd", index=False)
            geo_chunk_files.append(chunk_path)
            total += len(gdf_chunk)
            chunk_num += 1
            if chunk_num % 50 == 0:
                print(f"    chunk {chunk_num}: {total:,} rows")

        print(f"    merging {len(geo_chunk_files)} chunks...")
        first = pq.read_table(str(geo_chunk_files[0]))
        with pq.ParquetWriter(
            str(output_path), first.schema,
            compression="zstd", compression_level=6,
            use_dictionary=True, write_statistics=True,
        ) as writer:
            writer.write_table(first)
            for cf in geo_chunk_files[1:]:
                writer.write_table(pq.read_table(str(cf)))

        for cf in geo_chunk_files:
            cf.unlink()
        temp_dir.rmdir()

        row_count = pq.ParquetFile(str(output_path)).metadata.num_rows
        size_mb = output_path.stat().st_size / (1024 * 1024)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB)")
    else:
        print(f"  geo  ({variant}): reusing geometry from raw, swapping band_value...")
        raw_geo_path = FORMAT_PATHS["geoparquet"]["raw"]
        if not raw_geo_path.exists():
            _build_geoparquet("raw")

        flat_path = FORMAT_PATHS["parquet_flat"][variant]
        if not flat_path.exists():
            _build_flat(variant)

        raw_pf = pq.ParquetFile(str(raw_geo_path))
        flat_pf = pq.ParquetFile(str(flat_path))
        geo_field = raw_pf.schema_arrow.field("geometry")
        out_schema = pa.schema([
            pa.field("band_value", pa.int32()),
            geo_field,
        ])

        def _reuse_generator():
            for raw_batch, flat_batch in zip(
                raw_pf.iter_batches(batch_size=500_000),
                flat_pf.iter_batches(batch_size=500_000),
            ):
                yield pa.table({
                    "band_value": flat_batch.column("band_value"),
                    "geometry": raw_batch.column("geometry"),
                }, schema=out_schema)

        row_count, size_mb, elapsed = _write_batches_to_parquet(_reuse_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")


# ======================================================================
# Reporting
# ======================================================================

def record_file_sizes():
    sizes = {}
    for variant in DATA_VARIANTS:
        variant_sizes = {}
        for fmt_key, fmt_paths in FORMAT_PATHS.items():
            path = fmt_paths[variant]
            if path.exists():
                variant_sizes[FORMAT_LABELS[fmt_key]] = round(
                    path.stat().st_size / (1024 * 1024), 1
                )
            else:
                variant_sizes[FORMAT_LABELS[fmt_key]] = 0.0
        sizes[variant] = variant_sizes
    return sizes


# ======================================================================
# Main
# ======================================================================

def main():
    print("=== Stage 1: Data Preparation ===")
    t0 = time.time()

    # 1. Download raw COG
    download_copernicus_dem()

    # 2. Build smoothed COGs
    build_smoothed_cogs()

    # 3. Run-length analysis
    analyze_run_lengths()

    # 4. Build all parquet variants
    for variant in DATA_VARIANTS:
        print(f"\n--- Variant: {variant} ---")
        _build_flat(variant)
        _build_s2(variant)
        _build_h3(variant)
        _build_geoparquet(variant)

    # 5. Report
    sizes = record_file_sizes()
    print("\nFile sizes:")
    for variant in DATA_VARIANTS:
        print(f"  [{variant}]")
        for name, size_mb in sizes[variant].items():
            print(f"    {name}: {size_mb:.1f} MB")

    missing = []
    for variant in DATA_VARIANTS:
        for fmt_paths in FORMAT_PATHS.values():
            p = fmt_paths[variant]
            if not p.exists():
                missing.append(str(p))
    if missing:
        print(f"\nWARNING: {len(missing)} missing files")
        for p in missing:
            print(f"  - {p}")

    elapsed = time.time() - t0
    print(f"\nData prep complete in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
