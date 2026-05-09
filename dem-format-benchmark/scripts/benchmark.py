"""Stage 2: Run format x tool x query_type benchmarks with memray profiling.

Each combination is executed inside a subprocess (for timeout/isolation),
wrapped by memray.Tracker. Results are collected and saved as Parquet.
"""

import json
import multiprocessing
import subprocess
import sys
import time
import traceback
from pathlib import Path

import numpy as np
import pandas as pd
import psutil
from shapely.geometry import shape, box as shapely_box

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (  # noqa: E402
    BENCHMARK_COMBOS,
    BBOX_SIZE_DEG,
    COG_PATH,
    DATA_DIR,
    FLAT_PARQUET_PATH,
    FORMAT_PATHS,
    FORMAT_LABELS,
    GEOPARQUET_PATH,
    H3_PARQUET_PATH,
    H3_RESOLUTION,
    MEMRAY_DIR,
    NUM_RUNS,
    RESULTS_DIR,
    REGION_BOUNDS,
    S2_LEVEL,
    S2_PARQUET_PATH,
    SAMPLE_POLYGON_GEOJSON,
    TIMEOUT_SECONDS,
    TOOL_LABELS,
    generate_query_bboxes,
    generate_query_points,
)

# ---------------------------------------------------------------------------
# Pre-computed query datasets (materialised once at module level).
# In spawned subprocesses the module is re-imported, so each child gets fresh
# copies without shared-memory side-effects.
# ---------------------------------------------------------------------------
QUERY_POINTS = generate_query_points()          # list[tuple[float, float]]
QUERY_BBOXES = generate_query_bboxes()          # list[tuple[float,float,float,float]]

# ---------------------------------------------------------------------------
# Import availability check
# ---------------------------------------------------------------------------


def check_imports() -> set[str]:
    """Return the set of tool keys whose Python packages are importable."""
    available: set[str] = set()
    tool_imports: dict[str, str] = {
        "rasterio": "rasterio",
        "rioxarray": "rioxarray",
        "duckdb": "duckdb",
        "polars": "polars",
        "pandas": "pandas",
        "geopandas": "geopandas",
    }
    for tool, module in tool_imports.items():
        try:
            __import__(module)
            available.add(tool)
        except ImportError:
            pass
    return available


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _geojson_to_wkt(geojson_dict: dict) -> str:
    """Convert a GeoJSON geometry dict to a WKT string."""
    geom = shape(geojson_dict)
    return geom.wkt


def _polygon_bbox(polygon_geojson: dict) -> tuple[float, float, float, float]:
    """Return (minx, miny, maxx, maxy) for a GeoJSON polygon."""
    geom = shape(polygon_geojson)
    return geom.bounds  # type: ignore[return-value]


def _filesize_mb(format_key: str) -> float:
    """On-disk size of a format file in MiB."""
    path = FORMAT_PATHS.get(format_key)
    if path and path.exists():
        return round(path.stat().st_size / (1024 * 1024), 2)
    return 0.0


# ---------------------------------------------------------------------------
# Query implementations
# ---------------------------------------------------------------------------


def _query_cog(tool_key: str, query_type: str) -> None:
    """Execute a spatial query against the COG file."""
    cog = str(COG_PATH)

    if tool_key == "rasterio":
        import rasterio
        from rasterio import mask as rast_mask

        if query_type == "point":
            with rasterio.open(cog) as src:
                pts = QUERY_POINTS[:10]
                for lon, lat in pts:
                    for val in src.sample([(lon, lat)]):
                        _ = val
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            with rasterio.open(cog) as src:
                from rasterio.windows import from_bounds
                window = from_bounds(*bbox, src.transform)
                _ = src.read(1, window=window)
        elif query_type == "polygon":
            with rasterio.open(cog) as src:
                poly = shape(SAMPLE_POLYGON_GEOJSON)
                # rasterio.mask expects geometries in the CRS of the raster
                masked, _ = rast_mask.mask(src, [poly], crop=True)
                _ = masked

    elif tool_key == "rioxarray":
        import rioxarray  # noqa: F401 — registers .rio accessor
        import xarray as xr

        if query_type == "point":
            ds = xr.open_dataset(cog, engine="rasterio")
            pts = QUERY_POINTS[:10]
            for lon, lat in pts:
                _ = ds.sel(x=lon, y=lat, method="nearest")
            ds.close()
        elif query_type == "bbox":
            ds = xr.open_dataset(cog, engine="rasterio")
            bbox = QUERY_BBOXES[0]
            _ = ds.sel(
                x=slice(bbox[0], bbox[2]),
                y=slice(bbox[3], bbox[1]),
            )
            ds.close()
        elif query_type == "polygon":
            ds = xr.open_dataset(cog, engine="rasterio")
            poly = shape(SAMPLE_POLYGON_GEOJSON)
            # rioxarray .clip expects a GeoJSON-like dict
            clipped = ds.rio.clip([poly.__geo_interface__], ds.rio.crs)
            _ = clipped
            ds.close()

    elif tool_key == "duckdb":
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        wkt = _geojson_to_wkt(SAMPLE_POLYGON_GEOJSON)

        if query_type == "point":
            pts = QUERY_POINTS[:10]
            for lon, lat in pts:
                con.execute(
                    """SELECT (ST_DumpAsPolygons(rast)).val
                       FROM ST_Read(?) AS rast
                       WHERE ST_Contains(
                           ST_MakeEnvelope(ST_MinX(rast.rast_bounds),
                                           ST_MinY(rast.rast_bounds),
                                           ST_MaxX(rast.rast_bounds),
                                           ST_MaxY(rast.rast_bounds)),
                           ST_MakePoint(?, ?)
                       )""",
                    [cog, lon, lat],
                ).fetchall()
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            con.execute(
                """SELECT (ST_DumpAsPolygons(rast)).val
                   FROM ST_Read(?) AS rast
                   WHERE ST_Intersects(
                       ST_MakeEnvelope(
                           ST_MinX(rast.rast_bounds),
                           ST_MinY(rast.rast_bounds),
                           ST_MaxX(rast.rast_bounds),
                           ST_MaxY(rast.rast_bounds)
                       ),
                       ST_MakeEnvelope(?, ?, ?, ?)
                   )""",
                [cog, bbox[0], bbox[1], bbox[2], bbox[3]],
            ).fetchall()
        elif query_type == "polygon":
            con.execute(
                """SELECT (ST_DumpAsPolygons(rast)).val
                   FROM ST_Read(?) AS rast
                   WHERE ST_Intersects(
                       ST_MakeEnvelope(
                           ST_MinX(rast.rast_bounds),
                           ST_MinY(rast.rast_bounds),
                           ST_MaxX(rast.rast_bounds),
                           ST_MaxY(rast.rast_bounds)
                       ),
                       ST_GeomFromText(?)
                   )""",
                [cog, wkt],
            ).fetchall()
        con.close()


def _query_parquet(format_key: str, tool_key: str, query_type: str) -> None:
    """Execute a spatial query against a (flat | S2 | H3) Parquet file."""
    path = str(FORMAT_PATHS[format_key])
    is_s2 = format_key == "parquet_s2"
    is_h3 = format_key == "parquet_h3"
    has_cell = is_s2 or is_h3

    # Helper: pre-compute cell IDs covering a bbox / polygon
    def _s2_cells_for_bbox(bbox):
        import s2cell
        return s2cell.s2cell.get_covering(
            bbox[1], bbox[0], bbox[3], bbox[2], S2_LEVEL,
        )

    def _s2_cells_for_polygon(polygon_geojson):
        import s2cell
        b = _polygon_bbox(polygon_geojson)
        return s2cell.s2cell.get_covering(b[1], b[0], b[3], b[2], S2_LEVEL)

    def _s2_cell_for_point(lon, lat):
        import s2cell
        return s2cell.s2cell.lat_lon_to_cell_id(lat, lon, S2_LEVEL)

    def _h3_cells_for_bbox(bbox):
        import h3
        # Convert bbox to a GeoJSON polygon
        poly = shapely_box(bbox[0], bbox[1], bbox[2], bbox[3])
        return h3.polygon_to_cells(
            h3.geo_to_h3shape(poly.__geo_interface__), H3_RESOLUTION,
        )

    def _h3_cells_for_polygon(polygon_geojson):
        import h3
        return h3.polygon_to_cells(
            h3.geo_to_h3shape(polygon_geojson), H3_RESOLUTION,
        )

    def _h3_cell_for_point(lon, lat):
        import h3
        return h3.latlng_to_cell(lat, lon, H3_RESOLUTION)

    # Pre-compute cell ID lists for S2/H3 queries
    cell_ids_before = None
    if has_cell and query_type == "bbox":
        bbox = QUERY_BBOXES[0]
        if is_s2:
            cell_ids_before = [int(c) for c in _s2_cells_for_bbox(bbox)]
        else:
            cell_ids_before = [int(c) for c in _h3_cells_for_bbox(bbox)]
    elif has_cell and query_type == "polygon":
        if is_s2:
            cell_ids_before = [int(c) for c in _s2_cells_for_polygon(SAMPLE_POLYGON_GEOJSON)]
        else:
            cell_ids_before = [int(c) for c in _h3_cells_for_polygon(SAMPLE_POLYGON_GEOJSON)]

    if tool_key == "duckdb":
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        if query_type == "point":
            pts = QUERY_POINTS[:10]
            if is_s2:
                cell_ids = [int(_s2_cell_for_point(lon, lat)) for lon, lat in pts]
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE s2_cell IN ({','.join(map(str, cell_ids))})"
                ).fetchall()
            elif is_h3:
                cell_ids = [int(_h3_cell_for_point(lon, lat)) for lon, lat in pts]
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE h3_cell IN ({','.join(map(str, cell_ids))})"
                ).fetchall()
            else:
                for lon, lat in pts:
                    con.execute(
                        f"SELECT band_value FROM '{path}' WHERE x = ? AND y = ?",
                        [lon, lat],
                    ).fetchall()
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            if is_s2:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE s2_cell IN ({','.join(map(str, cell_ids_before))})"
                ).fetchall()
            elif is_h3:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE h3_cell IN ({','.join(map(str, cell_ids_before))})"
                ).fetchall()
            else:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE x >= ? AND x <= ? AND y >= ? AND y <= ?",
                    [bbox[0], bbox[2], bbox[1], bbox[3]],
                ).fetchall()
        elif query_type == "polygon":
            if is_s2:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE s2_cell IN ({','.join(map(str, cell_ids_before))})"
                ).fetchall()
            elif is_h3:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE h3_cell IN ({','.join(map(str, cell_ids_before))})"
                ).fetchall()
            else:
                pb = _polygon_bbox(SAMPLE_POLYGON_GEOJSON)
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE x >= ? AND x <= ? AND y >= ? AND y <= ?",
                    [pb[0], pb[2], pb[1], pb[3]],
                ).fetchall()
        con.close()

    elif tool_key == "polars":
        import polars as pl

        if query_type == "point":
            pts = QUERY_POINTS[:10]
            df = pl.scan_parquet(path)
            if is_s2:
                cell_ids = [int(_s2_cell_for_point(lon, lat)) for lon, lat in pts]
                result = df.filter(pl.col("s2_cell").is_in(cell_ids)).collect()
            elif is_h3:
                cell_ids = [int(_h3_cell_for_point(lon, lat)) for lon, lat in pts]
                result = df.filter(pl.col("h3_cell").is_in(cell_ids)).collect()
            else:
                lons, lats = zip(*pts)
                result = df.filter(
                    pl.col("x").is_in(list(lons)) & pl.col("y").is_in(list(lats))
                ).collect()
            _ = result
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            df = pl.scan_parquet(path)
            if is_s2:
                result = df.filter(pl.col("s2_cell").is_in(cell_ids_before)).collect()
            elif is_h3:
                result = df.filter(pl.col("h3_cell").is_in(cell_ids_before)).collect()
            else:
                result = df.filter(
                    (pl.col("x") >= bbox[0])
                    & (pl.col("x") <= bbox[2])
                    & (pl.col("y") >= bbox[1])
                    & (pl.col("y") <= bbox[3])
                ).collect()
            _ = result
        elif query_type == "polygon":
            df = pl.scan_parquet(path)
            if is_s2:
                result = df.filter(pl.col("s2_cell").is_in(cell_ids_before)).collect()
            elif is_h3:
                result = df.filter(pl.col("h3_cell").is_in(cell_ids_before)).collect()
            else:
                pb = _polygon_bbox(SAMPLE_POLYGON_GEOJSON)
                result = df.filter(
                    (pl.col("x") >= pb[0])
                    & (pl.col("x") <= pb[2])
                    & (pl.col("y") >= pb[1])
                    & (pl.col("y") <= pb[3])
                ).collect()
            _ = result

    elif tool_key == "pandas":
        if query_type == "point":
            pts = QUERY_POINTS[:10]
            if is_s2:
                cell_ids = [int(_s2_cell_for_point(lon, lat)) for lon, lat in pts]
                df = pd.read_parquet(path, filters=[("s2_cell", "in", cell_ids)])
            elif is_h3:
                cell_ids = [int(_h3_cell_for_point(lon, lat)) for lon, lat in pts]
                df = pd.read_parquet(path, filters=[("h3_cell", "in", cell_ids)])
            else:
                lons, lats = zip(*pts)
                df = pd.read_parquet(path)
                df = df[df["x"].isin(lons) & df["y"].isin(lats)]
            _ = df
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            if is_s2:
                df = pd.read_parquet(path, filters=[("s2_cell", "in", cell_ids_before)])
            elif is_h3:
                df = pd.read_parquet(path, filters=[("h3_cell", "in", cell_ids_before)])
            else:
                df = pd.read_parquet(
                    path,
                    filters=[
                        ("x", ">=", bbox[0]),
                        ("x", "<=", bbox[2]),
                        ("y", ">=", bbox[1]),
                        ("y", "<=", bbox[3]),
                    ],
                )
            _ = df
        elif query_type == "polygon":
            if is_s2:
                df = pd.read_parquet(path, filters=[("s2_cell", "in", cell_ids_before)])
            elif is_h3:
                df = pd.read_parquet(path, filters=[("h3_cell", "in", cell_ids_before)])
            else:
                pb = _polygon_bbox(SAMPLE_POLYGON_GEOJSON)
                df = pd.read_parquet(
                    path,
                    filters=[
                        ("x", ">=", pb[0]),
                        ("x", "<=", pb[2]),
                        ("y", ">=", pb[1]),
                        ("y", "<=", pb[3]),
                    ],
                )
            _ = df


def _query_geoparquet(tool_key: str, query_type: str) -> None:
    """Execute a spatial query against the GeoParquet file."""
    path = str(GEOPARQUET_PATH)

    if tool_key == "duckdb":
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        if query_type == "point":
            pts = QUERY_POINTS[:10]
            for lon, lat in pts:
                con.execute(
                    f"""SELECT band_value FROM '{path}'
                        WHERE ST_Within(
                            geometry,
                            ST_Buffer(ST_MakePoint(?, ?), 0.000001)
                        )""",
                    [lon, lat],
                ).fetchall()
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            con.execute(
                f"""SELECT band_value FROM '{path}'
                    WHERE ST_Within(
                        geometry,
                        ST_MakeEnvelope(?, ?, ?, ?)
                    )""",
                [bbox[0], bbox[1], bbox[2], bbox[3]],
            ).fetchall()
        elif query_type == "polygon":
            wkt = _geojson_to_wkt(SAMPLE_POLYGON_GEOJSON)
            con.execute(
                f"""SELECT band_value FROM '{path}'
                    WHERE ST_Within(geometry, ST_GeomFromText(?))""",
                [wkt],
            ).fetchall()
        con.close()

    elif tool_key == "geopandas":
        import geopandas as gpd

        gdf = gpd.read_parquet(path)

        if query_type == "point":
            pts = QUERY_POINTS[:10]
            for lon, lat in pts:
                point = shape({"type": "Point", "coordinates": [lon, lat]})
                result = gdf[gdf.geometry.distance(point) < 1e-8]
                _ = result
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            bounds_poly = shapely_box(*bbox)
            result = gdf[gdf.geometry.intersects(bounds_poly)]
            _ = result
        elif query_type == "polygon":
            poly = shape(SAMPLE_POLYGON_GEOJSON)
            result = gdf[gdf.geometry.within(poly)]
            _ = result


# ---------------------------------------------------------------------------
# Core benchmark runner (runs inside the child process)
# ---------------------------------------------------------------------------


def _execute_benchmark(format_key: str, tool_key: str, query_type: str) -> dict:
    """Dispatch to the correct query function, measure wall-clock time and RSS.

    Returns a result dict consumed by the parent process.
    """
    process = psutil.Process()
    t0 = time.perf_counter()

    if format_key == "cog":
        _query_cog(tool_key, query_type)
    elif format_key in ("parquet_flat", "parquet_s2", "parquet_h3"):
        _query_parquet(format_key, tool_key, query_type)
    elif format_key == "geoparquet":
        _query_geoparquet(tool_key, query_type)
    else:
        raise ValueError(f"Unknown format_key: {format_key}")

    duration_ms = (time.perf_counter() - t0) * 1000
    peak_memory_mb = process.memory_info().rss / (1024 * 1024)

    return {
        "format": format_key,
        "format_label": FORMAT_LABELS.get(format_key, format_key),
        "tool": tool_key,
        "tool_label": TOOL_LABELS.get(tool_key, tool_key),
        "query_type": query_type,
        "duration_ms": round(duration_ms, 2),
        "peak_memory_mb": round(peak_memory_mb, 2),
        "filesize_mb": _filesize_mb(format_key),
    }


# ---------------------------------------------------------------------------
# Subprocess worker (invoked via multiprocessing.Process)
# ---------------------------------------------------------------------------


def _benchmark_worker(
    queue: multiprocessing.Queue,
    format_key: str,
    tool_key: str,
    query_type: str,
    report_path: str,
) -> None:
    """Entry-point for the child process.  Wraps the query in memray.Tracker."""
    import memray
    try:
        with memray.Tracker(report_path):
            result = _execute_benchmark(format_key, tool_key, query_type)
        queue.put({"status": "success", **result})
    except Exception as exc:
        queue.put({
            "status": "crash",
            "format": format_key,
            "format_label": FORMAT_LABELS.get(format_key, format_key),
            "tool": tool_key,
            "tool_label": TOOL_LABELS.get(tool_key, tool_key),
            "query_type": query_type,
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        })


# ---------------------------------------------------------------------------
# Timeout-protected runner (called from the parent process)
# ---------------------------------------------------------------------------


def _run_with_memray(
    format_key: str, tool_key: str, query_type: str, run_index: int,
) -> dict:
    """Spawn a subprocess that benchmarks *format_key / tool_key / query_type*.

    The subprocess is killed if it exceeds *TIMEOUT_SECONDS*.  Returns a
    result dict that always includes a ``status`` key.
    """
    MEMRAY_DIR.mkdir(parents=True, exist_ok=True)
    report_path = str(
        MEMRAY_DIR / f"{format_key}__{tool_key}__{query_type}__run{run_index:02d}.bin"
    )

    ctx = multiprocessing.get_context("spawn")
    queue: multiprocessing.Queue = ctx.Queue()
    proc = ctx.Process(
        target=_benchmark_worker,
        args=(queue, format_key, tool_key, query_type, report_path),
    )
    proc.start()
    proc.join(timeout=TIMEOUT_SECONDS)

    if proc.is_alive():
        proc.kill()
        proc.join(timeout=5)
        return {
            "status": "timeout",
            "format": format_key,
            "format_label": FORMAT_LABELS.get(format_key, format_key),
            "tool": tool_key,
            "tool_label": TOOL_LABELS.get(tool_key, tool_key),
            "query_type": query_type,
            "duration_ms": None,
            "peak_memory_mb": None,
            "filesize_mb": _filesize_mb(format_key),
            "run": run_index,
        }

    try:
        result = queue.get_nowait()
    except Exception:
        return {
            "status": "crash",
            "format": format_key,
            "format_label": FORMAT_LABELS.get(format_key, format_key),
            "tool": tool_key,
            "tool_label": TOOL_LABELS.get(tool_key, tool_key),
            "query_type": query_type,
            "duration_ms": None,
            "peak_memory_mb": None,
            "filesize_mb": _filesize_mb(format_key),
            "run": run_index,
            "error": "Subprocess died without writing to queue",
        }

    result["run"] = run_index
    return result


# ---------------------------------------------------------------------------
# Memray peak extraction (post-hoc, for optional enrichment)
# ---------------------------------------------------------------------------


def _read_memray_peak(report_path: str) -> float | None:
    """Extract the peak memory (MiB) recorded by a memray report.

    Uses ``python -m memray stats`` CLI since the Python FileReader API
    for peak extraction is verbose.  Returns *None* if the report is missing
    or parsing fails.
    """
    rp = Path(report_path)
    if not rp.exists():
        return None
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "memray", "stats", str(rp)],
            capture_output=True, text=True, timeout=30,
        )
        for line in proc.stdout.splitlines():
            # memray >=1.15 emits a line like "Peak memory: 123.45 MB"
            if "Peak memory" in line or "peak memory" in line:
                # Pull out the numeric part
                parts = line.replace(",", "").split()
                for p in parts:
                    stripped = p.rstrip("MBmb")
                    try:
                        return float(stripped)
                    except ValueError:
                        continue
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    print("=== Stage 2: Benchmarking ===")
    t_total = time.time()

    available_tools = check_imports()
    print(f"Available tools: {', '.join(sorted(available_tools))}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    MEMRAY_DIR.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    total_combos = len(BENCHMARK_COMBOS) * NUM_RUNS
    completed = 0

    for format_key, tool_key, query_type in BENCHMARK_COMBOS:
        # Skip if tool is not installed
        if tool_key not in available_tools:
            msg = f"SKIP  {format_key:20s} | {tool_key:12s} | {query_type:8s} (tool unavailable)"
            print(msg)
            continue

        # Verify data file exists
        data_path = FORMAT_PATHS.get(format_key)
        if data_path is None or not data_path.exists():
            msg = f"SKIP  {format_key:20s} | {tool_key:12s} | {query_type:8s} (data missing: {data_path})"
            print(msg)
            continue

        for run_idx in range(NUM_RUNS):
            completed += 1
            label = f"{format_key} | {tool_key} | {query_type}"
            print(f"[{completed}/{total_combos}] {label}  run {run_idx + 1}/{NUM_RUNS} ...", end=" ", flush=True)

            t_run = time.time()
            row = _run_with_memray(format_key, tool_key, query_type, run_idx)

            elapsed_run = time.time() - t_run
            if row["status"] == "success":
                print(f"OK  {row['duration_ms']:.0f} ms  {row['peak_memory_mb']:.0f} MiB")
            elif row["status"] == "timeout":
                print(f"TIMEOUT (>{TIMEOUT_SECONDS}s)")
            else:
                err_detail = row.get("error", "unknown")
                print(f"CRASH  ({err_detail[:80]})")

            # Optional: enrich with memray peak
            report_path = str(
                MEMRAY_DIR / f"{format_key}__{tool_key}__{query_type}__run{run_idx:02d}.bin"
            )
            memray_peak = _read_memray_peak(report_path)
            if memray_peak is not None:
                row["memray_peak_mb"] = round(memray_peak, 2)

            row["wall_seconds"] = round(elapsed_run, 2)
            all_rows.append(row)

    # ------------------------------------------------------------------
    # Save results
    # ------------------------------------------------------------------
    df = pd.DataFrame(all_rows)
    output_path = RESULTS_DIR / "benchmarks.parquet"
    df.to_parquet(output_path, index=False)
    print(f"\nSaved {len(df)} rows to {output_path}")

    # Quick summary
    success_df = df[df["status"] == "success"]
    if len(success_df) > 0:
        print(f"\nSuccessful runs: {len(success_df)}")
        print(f"  Median duration: {success_df['duration_ms'].median():.1f} ms")
        print(f"  Median peak RSS: {success_df['peak_memory_mb'].median():.1f} MiB")

    crash_count = len(df[df["status"] == "crash"])
    timeout_count = len(df[df["status"] == "timeout"])
    if crash_count:
        print(f"  Crashes: {crash_count}")
    if timeout_count:
        print(f"  Timeouts: {timeout_count}")

    elapsed_total = time.time() - t_total
    print(f"\nBenchmarking complete in {elapsed_total:.0f}s")


if __name__ == "__main__":
    main()
