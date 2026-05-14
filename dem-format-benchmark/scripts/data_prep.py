"""Stage 1: Download Copernicus GLO-30 DEM COG, quantize, and convert to all target formats.

Uses Python/Rasterio for the initial extraction to a temporary base Parquet,
and DuckDB/Polars for generating geometries, H3, S2, and optimized Parquets.
"""

import time
import sys
from pathlib import Path
import rasterio
from rasterio.windows import Window
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    DATA_DIR, DATA_VARIANTS, REGION_BOUNDS, S2_LEVEL, H3_RESOLUTION,
    FORMAT_PATHS, FORMAT_LABELS, TABLES_DIR,
)
def _quantize_band_values(values):
    return np.round(values.astype(np.float32, copy=False)).astype(np.int16)


def _valid_data_mask(data, nodata):
    valid_mask = np.isfinite(data)
    if nodata is not None and np.isfinite(nodata):
        valid_mask &= data != nodata
    return valid_mask

def _is_parquet_valid(path: Path) -> bool:
    """Check if a Parquet file exists and is readable."""
    if not path.exists():
        return False
    import pyarrow.parquet as pq
    try:
        pq.ParquetFile(str(path))
        return True
    except Exception:
        print(f"  WARNING: {path.name} is corrupted, deleting and rebuilding...")
        path.unlink()
        return False

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

def _create_base_parquet():
    """Extract valid pixels from the raw COG into a fast, unsorted base Parquet file."""
    base_raw_path = DATA_DIR / "dem_base_raw.parquet"
    if base_raw_path.exists():
        return base_raw_path
        
    cog_path = FORMAT_PATHS["cog"]["raw"]
    print(f"Creating base Parquet from {cog_path.name}...")
    
    schema = pa.schema([
        pa.field("col", pa.uint32()),
        pa.field("row", pa.uint32()),
        pa.field("band_value", pa.int16()),
    ])
    
    with rasterio.open(cog_path) as src:
        width, height = src.width, src.height
        transform = src.transform
        strip_size = 256
        
        with pq.ParquetWriter(str(base_raw_path), schema, compression="lz4") as writer:
            from tqdm import tqdm
            for y_start in tqdm(range(0, height, strip_size), desc="  Extracting"):
                y_end = min(y_start + strip_size, height)
                window = Window(0, y_start, width, y_end - y_start)
                data = src.read(1, window=window)
                valid_mask = _valid_data_mask(data, src.nodata)
                if not valid_mask.any():
                    continue

                rows, cols = np.nonzero(valid_mask)
                table = pa.table([
                    pa.array((cols + window.col_off).astype(np.uint32)),
                    pa.array((rows + window.row_off).astype(np.uint32)),
                    pa.array(_quantize_band_values(data[rows, cols]))
                ], schema=schema)
                writer.write_table(table)
                
    print(f"  Wrote {base_raw_path.name} ({base_raw_path.stat().st_size / (1024*1024):.1f} MB)")
    return base_raw_path


def build_duckdb_variants(variant: str, base_raw_path: Path):
    """Use DuckDB to generate flat, H3, and GeoParquet from the base Parquet."""
    import duckdb
    import pyarrow.parquet as pq
    
    con = duckdb.connect()
    con.execute("INSTALL spatial; LOAD spatial; INSTALL h3; LOAD h3;")
    
    # Read the base parquet and get transform
    cog_path = FORMAT_PATHS["cog"]["raw"]
    with rasterio.open(cog_path) as src:
        tr = src.transform
        # Create a view that computes actual x, y
        # and applies quantization if variant is q*
        quantize_expr = "CAST(band_value AS SMALLINT)"
        if variant.startswith("q"):
            q_val = int(variant[1:])
            quantize_expr = f"CAST(ROUND(band_value / {q_val}.0) * {q_val} AS SMALLINT)"
            
        con.execute(f"""
            CREATE VIEW base AS 
            SELECT 
                {tr.c} + (col + 0.5) * {tr.a} AS x,
                {tr.f} + (row + 0.5) * {tr.e} AS y,
                {quantize_expr} AS band_value
            FROM read_parquet('{base_raw_path}')
        """)

    # 1. Parquet Flat (sorted by S2 to maintain some spatial locality)
    flat_path = FORMAT_PATHS["parquet_flat"][variant]
    if not _is_parquet_valid(flat_path):
        print(f"  [{variant}] writing flat parquet (DuckDB)...")
        con.execute(f"""
            COPY (
                SELECT x, y, band_value 
                FROM base 
            ) TO '{flat_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)

    # 2. GeoParquet
    geo_path = FORMAT_PATHS["geoparquet"][variant]
    if not _is_parquet_valid(geo_path):
        print(f"  [{variant}] writing geoparquet (DuckDB)...")
        con.execute(f"""
            COPY (
                SELECT ST_Point(x, y) AS geometry, band_value 
                FROM base
            ) TO '{geo_path}' (FORMAT PARQUET, COMPRESSION ZSTD);
        """)


def build_hilbert_variant(variant: str, base_raw_path: Path):
    """Use Polars and Rust plugin to generate Z-order Parquet."""
    import polars as pl
    import pyarrow.parquet as pq
    import sys
    from pathlib import Path
    
    scripts_dir = str(Path(__file__).resolve().parent)
    if scripts_dir not in sys.path:
        sys.path.append(scripts_dir)
        
    from scripts.hilbert_plugin import compute_hilbert, compact_hilbert
    
    hilbert_path = FORMAT_PATHS["parquet_hilbert"][variant]
    if _is_parquet_valid(hilbert_path):
        return

    print(f"  [{variant}] writing hilbert parquet (Polars/Rust)...")
    
    cog_path = FORMAT_PATHS["cog"]["raw"]
    with rasterio.open(cog_path) as src:
        transform = src.transform

    quantize_expr = pl.col("band_value").cast(pl.Int16)
    if variant.startswith("q"):
        q_val = int(variant[1:])
        quantize_expr = ((pl.col("band_value") / float(q_val)).round() * q_val).cast(pl.Int16)

    # Stream from the base parquet
    q = (
        pl.scan_parquet(str(base_raw_path))
        .with_columns([
            quantize_expr.alias("band_value"),
            (((transform.c + pl.col("col") * transform.a) + 180.0) * 3600.0).round().cast(pl.UInt32).alias("global_col"),
            ((90.0 - (transform.f + pl.col("row") * transform.e)) * 3600.0).round().cast(pl.UInt32).alias("global_row")
        ])
        .with_columns([
            compute_hilbert("global_col", "global_row").alias("z_index")
        ])
        .group_by("band_value")
        .agg(pl.col("z_index"))
        .with_columns(
            compact_hilbert(pl.col("z_index")).alias("z_index")
        )
        .explode("z_index")
        .sort("z_index")
        .select(["band_value", "z_index"])
    )
    
    q.sink_parquet(
        str(hilbert_path),
        compression="zstd",
    )


def record_file_sizes():
    sizes = {}
    for variant in DATA_VARIANTS:
        variant_sizes = {}
        for fmt_key, fmt_paths in FORMAT_PATHS.items():
            path = fmt_paths[variant]
            if path.exists():
                if path.is_dir():
                    # For Zarr directories, sum up all files
                    total_size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
                    variant_sizes[FORMAT_LABELS[fmt_key]] = round(total_size / (1024 * 1024), 1)
                else:
                    variant_sizes[FORMAT_LABELS[fmt_key]] = round(
                        path.stat().st_size / (1024 * 1024), 1
                    )
            else:
                variant_sizes[FORMAT_LABELS[fmt_key]] = 0.0
        sizes[variant] = variant_sizes
    return sizes


def build_variant_cogs():
    """Create processed COGs (quantized) from the raw COG."""
    cog_raw = FORMAT_PATHS["cog"]["raw"]
    if not cog_raw.exists():
        raise FileNotFoundError(f"Raw COG not found: {cog_raw}")

    # Process all variants except 'raw'
    for variant in DATA_VARIANTS:
        if variant == "raw":
            continue

        cog_out = FORMAT_PATHS["cog"][variant]
        if cog_out.exists():
            print(f"  COG {variant} already exists, skipping")
            continue

        if variant.startswith("q"):
            q_val = int(variant[1:])
            print(f"Applying quantization bucket size={q_val}...")
        else:
            continue

        with rasterio.open(cog_raw) as src:
            profile = src.profile.copy()
            profile.update(dtype=rasterio.int16, nodata=-32768)
            height, width = src.height, src.width
            strip_size = 256
            
            with rasterio.open(cog_out, "w", **profile) as dst:
                from tqdm import tqdm
                for y_start in tqdm(range(0, height, strip_size), desc=f"  {variant} strips"):
                    y_end = min(y_start + strip_size, height)
                    window = Window(0, y_start, width, y_end - y_start)
                    data = src.read(1, window=window)
                    
                    # Quantize data, ignoring NoData
                    out_data = np.full(data.shape, -32768, dtype=np.int16)
                    valid_mask = _valid_data_mask(data, src.nodata)
                    if valid_mask.any():
                        out_data[valid_mask] = np.round(data[valid_mask] / float(q_val)) * q_val
                        
                    dst.write(out_data, indexes=1, window=window)

        size_mb = cog_out.stat().st_size / (1024 * 1024)
        print(f"  Wrote {cog_out.name} ({size_mb:.1f} MB)")

def build_variant_zarrs():
    """Create Zarr archives from the COGs."""
    import xarray as xr
    import rioxarray

    for variant in DATA_VARIANTS:
        zarr_out = FORMAT_PATHS["zarr"][variant]
        if zarr_out.exists():
            print(f"  Zarr {variant} already exists, skipping")
            continue

        cog_path = FORMAT_PATHS["cog"][variant]
        print(f"  [{variant}] writing zarr...")
        
        # Open COG with rioxarray in chunks to avoid blowing up memory
        da = rioxarray.open_rasterio(cog_path, chunks={"x": 512, "y": 512})
        # Zarr recommends not having spatial dimensions in the chunking to be overly complex if not needed,
        # but matching the COG chunking (512x512) is a good start.
        da.to_zarr(zarr_out, mode="w", compute=True)

def build_variant_lance():
    """Create Lance datasets from the Hilbert Parquet files."""
    import lance
    import polars as pl
    print("Building Lance datasets...")
    for variant in DATA_VARIANTS:
        lance_out = FORMAT_PATHS["lance"][variant]
        if lance_out.exists():
            print(f"  Lance {variant} already exists, skipping")
            continue

        parquet_path = FORMAT_PATHS["parquet_hilbert"][variant]
        if not parquet_path.exists():
            print(f"  [{variant}] Hilbert parquet not found, skipping lance build")
            continue
            
        print(f"  [{variant}] writing lance...")
        df = pl.read_parquet(parquet_path)
        lance.write_dataset(df.to_arrow(), lance_out)

def main():
    print("=== Stage 1: Data Preparation ===")
    t0 = time.time()

    download_copernicus_dem()
    build_variant_cogs()
    build_variant_zarrs()
    base_raw_path = _create_base_parquet()

    for variant in DATA_VARIANTS:
        print(f"\n--- Variant: {variant} ---")
        build_duckdb_variants(variant, base_raw_path)
        build_hilbert_variant(variant, base_raw_path)
        
    build_variant_lance()

    sizes = record_file_sizes()
    print("\nFile sizes:")
    for variant in DATA_VARIANTS:
        print(f"  [{variant}]")
        for name, size_mb in sizes[variant].items():
            print(f"    {name}: {size_mb:.1f} MB")

    elapsed = time.time() - t0
    print(f"\nData prep complete in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
