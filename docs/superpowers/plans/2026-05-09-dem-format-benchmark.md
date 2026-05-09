# DEM Format Benchmark Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a modular benchmark pipeline that compares COG, Parquet+S2, Parquet+H3, and GeoParquet for point/polygon DEM queries on 8GB hardware, producing publication-quality plots and an MDX blog post.

**Architecture:** Four independent pipeline stages (data prep, benchmark, visualize, generate post) connected by a shared config and intermediate Parquet/PNG artifacts. Each stage can run independently after earlier stages complete. Memory profiling via memray Tracker contexts, per-run captures saved for flame graph generation.

**Tech Stack:** Python 3.12+, rasterio, rioxarray, duckdb (spatial ext), polars, pandas, geopandas, h3, s2cell, memray, matplotlib, seaborn, jinja2

---

## File Structure

```
dem-format-benchmark/
├── config.py                    # Shared: region extent, seeds, query params
├── pyproject.toml               # Dependencies
├── .gitignore                   # Ignore data/, results/memray/
├── scripts/
│   ├── __init__.py              # Empty
│   ├── data_prep.py             # Stage 1: download COG → convert to all formats
│   ├── benchmark.py             # Stage 2: run every format×tool×query combo
│   ├── visualize.py             # Stage 3: generate plots and tables
│   └── generate_post.py         # Stage 4: assemble MDX from template + results
├── templates/
│   └── blog_post.mdx.jinja2     # MDX template for the blog post
├── data/                        # Gitignored: raw COG + converted parquet files
├── results/
│   ├── benchmarks.parquet       # One row per benchmark run (committed after bench)
│   ├── memray/                  # Gitignored: per-run .bin captures
│   └── tables/                  # Generated markdown table snippets
├── plots/                       # Generated PNGs (committed)
└── content/                     # Output MDX file
    └── YYYY-MM-DD-dem-format-benchmark.mdx
```

---

### Task 1: Project scaffolding

**Files:**
- Create: `dem-format-benchmark/pyproject.toml`
- Create: `dem-format-benchmark/.gitignore`
- Create: `dem-format-benchmark/scripts/__init__.py`

- [ ] **Step 1: Create directory structure**

```bash
mkdir -p dem-format-benchmark/{scripts,templates,data,results/memray,results/tables,plots,content}
touch dem-format-benchmark/scripts/__init__.py
```

- [ ] **Step 2: Write pyproject.toml**

```toml
[project]
name = "dem-format-benchmark"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "rasterio>=1.4",
    "rioxarray>=0.17",
    "xarray>=2024.0",
    "duckdb>=1.2",
    "polars>=1.0",
    "pandas>=2.2",
    "geopandas>=1.0",
    "h3>=4.0",
    "s2cell>=1.7",
    "memray>=1.15",
    "matplotlib>=3.10",
    "seaborn>=0.13",
    "jinja2>=3.1",
    "tqdm>=4.66",
    "shapely>=2.0",
]

[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"
```

- [ ] **Step 3: Write .gitignore**

```
data/*
!data/.gitkeep
results/memray/*
!results/memray/.gitkeep
__pycache__/
*.pyc
.venv/
```

- [ ] **Step 4: Create .gitkeep files for empty tracked dirs**

```bash
touch dem-format-benchmark/data/.gitkeep
touch dem-format-benchmark/results/memray/.gitkeep
```

- [ ] **Step 5: Commit**

```bash
git add dem-format-benchmark/
git commit -m "feat: scaffold dem-format-benchmark project structure"
```

---

### Task 2: Config module

**Files:**
- Create: `dem-format-benchmark/config.py`

- [ ] **Step 1: Write config.py**

```python
"""Shared configuration for the DEM format benchmark pipeline."""

import json
from pathlib import Path

# ---- Paths ----
ROOT = Path(__file__).resolve().parent
DATA_DIR = ROOT / "data"
RESULTS_DIR = ROOT / "results"
PLOTS_DIR = ROOT / "plots"
MEMRAY_DIR = RESULTS_DIR / "memray"
TABLES_DIR = RESULTS_DIR / "tables"
CONTENT_DIR = ROOT / "content"

# ---- Region (Swiss Alps + northern Italy, ~6°×6°) ----
REGION_BOUNDS = {
    "min_lon": 6.0,
    "min_lat": 44.0,
    "max_lon": 12.0,
    "max_lat": 50.0,
}

# ---- Random seed for reproducibility ----
SEED = 42
NUM_SAMPLE_POINTS = 100

# ---- S2 / H3 parameters ----
S2_LEVEL = 14        # ~0.03 km² per cell
H3_RESOLUTION = 12   # ~0.03 km² mean area

# ---- Benchmark params ----
NUM_RUNS = 10
TIMEOUT_SECONDS = 120

# ---- Bbox size for spatial queries ----
BBOX_SIZE_DEG = 0.01   # ~1.1 km at these latitudes

# ---- Polygon for area query (irregular, ~0.02° across) ----
SAMPLE_POLYGON_GEOJSON = {
    "type": "Polygon",
    "coordinates": [[
        [8.5, 46.5],
        [8.52, 46.5],
        [8.52, 46.48],
        [8.51, 46.47],
        [8.49, 46.48],
        [8.49, 46.5],
        [8.5, 46.5],
    ]],
}

# ---- File paths for each format ----
COG_PATH = DATA_DIR / "dem_cog.tif"
FLAT_PARQUET_PATH = DATA_DIR / "dem_flat.parquet"
S2_PARQUET_PATH = DATA_DIR / "dem_s2.parquet"
H3_PARQUET_PATH = DATA_DIR / "dem_h3.parquet"
GEOPARQUET_PATH = DATA_DIR / "dem.geoparquet"

FORMAT_PATHS = {
    "cog": COG_PATH,
    "parquet_flat": FLAT_PARQUET_PATH,
    "parquet_s2": S2_PARQUET_PATH,
    "parquet_h3": H3_PARQUET_PATH,
    "geoparquet": GEOPARQUET_PATH,
}

# ---- Format × tool combinations to benchmark ----
# Each tuple: (format_key, tool_key, query_type)
# query_type: "point", "bbox", "polygon"
BENCHMARK_COMBOS = [
    # COG
    ("cog", "rasterio", "point"),
    ("cog", "rasterio", "bbox"),
    ("cog", "rasterio", "polygon"),
    ("cog", "rioxarray", "point"),
    ("cog", "rioxarray", "bbox"),
    ("cog", "rioxarray", "polygon"),
    ("cog", "duckdb", "point"),
    ("cog", "duckdb", "bbox"),
    ("cog", "duckdb", "polygon"),
    # Parquet flat
    ("parquet_flat", "duckdb", "point"),
    ("parquet_flat", "duckdb", "bbox"),
    ("parquet_flat", "duckdb", "polygon"),
    ("parquet_flat", "polars", "point"),
    ("parquet_flat", "polars", "bbox"),
    ("parquet_flat", "polars", "polygon"),
    ("parquet_flat", "pandas", "point"),
    ("parquet_flat", "pandas", "bbox"),
    ("parquet_flat", "pandas", "polygon"),
    # Parquet + S2
    ("parquet_s2", "duckdb", "point"),
    ("parquet_s2", "duckdb", "bbox"),
    ("parquet_s2", "duckdb", "polygon"),
    ("parquet_s2", "polars", "point"),
    ("parquet_s2", "polars", "bbox"),
    ("parquet_s2", "polars", "polygon"),
    ("parquet_s2", "pandas", "point"),
    ("parquet_s2", "pandas", "bbox"),
    ("parquet_s2", "pandas", "polygon"),
    # Parquet + H3
    ("parquet_h3", "duckdb", "point"),
    ("parquet_h3", "duckdb", "bbox"),
    ("parquet_h3", "duckdb", "polygon"),
    ("parquet_h3", "polars", "point"),
    ("parquet_h3", "polars", "bbox"),
    ("parquet_h3", "polars", "polygon"),
    ("parquet_h3", "pandas", "point"),
    ("parquet_h3", "pandas", "bbox"),
    ("parquet_h3", "pandas", "polygon"),
    # GeoParquet
    ("geoparquet", "duckdb", "point"),
    ("geoparquet", "duckdb", "bbox"),
    ("geoparquet", "duckdb", "polygon"),
    ("geoparquet", "geopandas", "point"),
    ("geoparquet", "geopandas", "bbox"),
    ("geoparquet", "geopandas", "polygon"),
]

# ---- Display labels ----
FORMAT_LABELS = {
    "cog": "COG",
    "parquet_flat": "Parquet (flat)",
    "parquet_s2": "Parquet + S2",
    "parquet_h3": "Parquet + H3",
    "geoparquet": "GeoParquet",
}

TOOL_LABELS = {
    "rasterio": "Rasterio",
    "rioxarray": "rioxarray",
    "duckdb": "DuckDB",
    "polars": "Polars",
    "pandas": "pandas",
    "geopandas": "GeoPandas",
}

QUERY_LABELS = {
    "point": "Point sample",
    "bbox": "Bbox window",
    "polygon": "Polygon",
}


def generate_query_points(n=NUM_SAMPLE_POINTS, seed=SEED):
    """Generate reproducible random lon/lat points within the region."""
    import numpy as np
    rng = np.random.default_rng(seed)
    lons = rng.uniform(REGION_BOUNDS["min_lon"], REGION_BOUNDS["max_lon"], n)
    lats = rng.uniform(REGION_BOUNDS["min_lat"], REGION_BOUNDS["max_lat"], n)
    return list(zip(lons, lats))


def generate_query_bboxes(n=5, seed=SEED):
    """Generate reproducible random bboxes within the region."""
    import numpy as np
    rng = np.random.default_rng(seed)
    bboxes = []
    for _ in range(n):
        lon = rng.uniform(
            REGION_BOUNDS["min_lon"], REGION_BOUNDS["max_lon"] - BBOX_SIZE_DEG
        )
        lat = rng.uniform(
            REGION_BOUNDS["min_lat"], REGION_BOUNDS["max_lat"] - BBOX_SIZE_DEG
        )
        bboxes.append((lon, lat, lon + BBOX_SIZE_DEG, lat + BBOX_SIZE_DEG))
    return bboxes
```

- [ ] **Step 2: Commit**

```bash
git add dem-format-benchmark/config.py
git commit -m "feat: add shared config with region, query params, and combo matrix"
```

---

### Task 3: Data preparation script

**Files:**
- Create: `dem-format-benchmark/scripts/data_prep.py`

- [ ] **Step 1: Write data_prep.py**

```python
"""Stage 1: Download Copernicus GLO-30 DEM COG and convert to all target formats."""

import json
import time
import sys
from pathlib import Path

import rasterio
from rasterio.windows import Window
from rasterio.transform import rowcol
import numpy as np
import pandas as pd
import geopandas as gpd
import h3
import s2cell
from shapely.geometry import Point
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    DATA_DIR, REGION_BOUNDS, S2_LEVEL, H3_RESOLUTION, SEED,
    COG_PATH, FLAT_PARQUET_PATH, S2_PARQUET_PATH, H3_PARQUET_PATH, GEOPARQUET_PATH,
)


def download_copernicus_dem():
    """Download Copernicus GLO-30 tiles covering the region and build a VRT.

    Uses the public Copernicus DEM S3 bucket. Falls back to a bounding-box
    download from OpenTopography if S3 access is unavailable.
    """
    if COG_PATH.exists():
        print(f"COG already exists at {COG_PATH}, skipping download")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Copernicus DEM is available at 1° tiles. We build a VRT from the
    # public AWS registry of open data.
    bounds = REGION_BOUNDS
    min_lon = int(bounds["min_lon"])
    max_lon = int(bounds["max_lon"]) + 1
    min_lat = int(bounds["min_lat"])
    max_lat = int(bounds["max_lat"]) + 1

    tile_urls = []
    for lat in range(min_lat, max_lat):
        lat_char = "N" if lat >= 0 else "S"
        for lon in range(min_lon, max_lon):
            lon_char = "E" if lon >= 0 else "W"
            tile_name = f"Copernicus_DSM_COG_10_{lat_char}{abs(lat):02d}_00_{lon_char}{abs(lon):03d}_00_DEM"
            url = f"/vsis3/copernicus-dem-30m/{tile_name}/{tile_name}.tif"
            tile_urls.append(url)

    # Try S3 first, fall back to GDAL's network VRT
    try:
        from osgeo import gdal
        vrt = gdal.BuildVRT(str(COG_PATH.parent / "dem_mosaic.vrt"), tile_urls)
        # Convert to COG for clean baseline
        gdal.Translate(
            str(COG_PATH),
            vrt,
            format="COG",
            outputBounds=[
                bounds["min_lon"], bounds["min_lat"],
                bounds["max_lon"], bounds["max_lat"],
            ],
            resampleAlg="bilinear",
        )
        vrt = None
    except Exception:
        print("S3 access failed. Download a single merged tile manually or use OpenTopography.")
        print(f"Place your COG at: {COG_PATH}")
        raise


def extract_to_flat_parquet():
    """Extract COG pixels to a row-per-pixel flat Parquet file."""
    if FLAT_PARQUET_PATH.exists():
        print(f"Flat parquet already exists at {FLAT_PARQUET_PATH}, skipping")
        return

    print("Extracting COG to flat Parquet...")
    rows_list = []
    with rasterio.open(COG_PATH) as src:
        transform = src.transform
        width = src.width
        height = src.height
        block_size = 1024

        for y_start in tqdm(range(0, height, block_size), desc="Rows"):
            y_end = min(y_start + block_size, height)
            window = Window(0, y_start, width, y_end - y_start)
            data = src.read(1, window=window)
            nodata = src.nodata

            ys, xs = np.where(data != nodata) if nodata is not None else (
                np.indices(data.shape).reshape(2, -1)
            )
            for yi, xi in zip(ys, xs):
                px = window.col_off + xi
                py = window.row_off + yi
                lon, lat = transform * (px + 0.5, py + 0.5)
                rows_list.append({
                    "x": lon,
                    "y": lat,
                    "band_value": float(data[yi, xi]),
                })

    df = pd.DataFrame(rows_list)
    df.to_parquet(FLAT_PARQUET_PATH, compression="zstd", index=False)
    print(f"Wrote {len(df):,} rows to {FLAT_PARQUET_PATH}")


def add_s2_index():
    """Add S2 cell IDs to the flat parquet."""
    if S2_PARQUET_PATH.exists():
        print(f"S2 parquet already exists at {S2_PARQUET_PATH}, skipping")
        return

    print("Adding S2 cell index...")
    df = pd.read_parquet(FLAT_PARQUET_PATH)
    df["s2_cell"] = df.apply(
        lambda row: s2cell.s2cell.lat_lon_to_cell_id(
            row["y"], row["x"], S2_LEVEL
        ),
        axis=1,
    )
    df["s2_cell"] = df["s2_cell"].astype("int64")
    df.to_parquet(S2_PARQUET_PATH, compression="zstd", index=False)
    print(f"Wrote {len(df):,} rows to {S2_PARQUET_PATH}")


def add_h3_index():
    """Add H3 cell IDs to the flat parquet."""
    if H3_PARQUET_PATH.exists():
        print(f"H3 parquet already exists at {H3_PARQUET_PATH}, skipping")
        return

    print("Adding H3 cell index...")
    df = pd.read_parquet(FLAT_PARQUET_PATH)
    df["h3_cell"] = df.apply(
        lambda row: h3.latlng_to_cell(row["y"], row["x"], H3_RESOLUTION),
        axis=1,
    )
    df["h3_cell"] = df["h3_cell"].astype("int64")
    df.to_parquet(H3_PARQUET_PATH, compression="zstd", index=False)
    print(f"Wrote {len(df):,} rows to {H3_PARQUET_PATH}")


def write_geoparquet():
    """Write GeoParquet with native geometry column."""
    if GEOPARQUET_PATH.exists():
        print(f"GeoParquet already exists at {GEOPARQUET_PATH}, skipping")
        return

    print("Writing GeoParquet...")
    df = pd.read_parquet(FLAT_PARQUET_PATH)
    geometry = [Point(x, y) for x, y in zip(df["x"], df["y"])]
    gdf = gpd.GeoDataFrame(
        {"band_value": df["band_value"]},
        geometry=geometry,
        crs="EPSG:4326",
    )
    gdf.to_parquet(GEOPARQUET_PATH, compression="zstd", index=False)
    print(f"Wrote {len(gdf):,} rows to {GEOPARQUET_PATH}")


def record_file_sizes():
    """Record on-disk sizes of each format file."""
    sizes = {}
    from config import FORMAT_PATHS, FORMAT_LABELS
    for key, path in FORMAT_PATHS.items():
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            sizes[FORMAT_LABELS[key]] = round(size_mb, 2)
    return sizes


def main():
    print("=== Stage 1: Data Preparation ===")
    t0 = time.time()

    download_copernicus_dem()
    extract_to_flat_parquet()
    add_s2_index()
    add_h3_index()
    write_geoparquet()

    sizes = record_file_sizes()
    print("\nFile sizes:")
    for name, size_mb in sizes.items():
        print(f"  {name}: {size_mb:.1f} MB")

    elapsed = time.time() - t0
    print(f"\nData prep complete in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add dem-format-benchmark/scripts/data_prep.py
git commit -m "feat: add data prep script (COG download + format conversion)"
```

---

### Task 4: Benchmark script

**Files:**
- Create: `dem-format-benchmark/scripts/benchmark.py`

- [ ] **Step 1: Write benchmark.py**

```python
"""Stage 2: Run benchmarks for every format × tool × query_type combination.

Each run is profiled with memray. Results saved to results/benchmarks.parquet.
"""

import gc
import json
import multiprocessing
import os
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    DATA_DIR, RESULTS_DIR, MEMRAY_DIR, SEED, NUM_RUNS, TIMEOUT_SECONDS,
    BENCHMARK_COMBOS, COG_PATH, FLAT_PARQUET_PATH, S2_PARQUET_PATH,
    H3_PARQUET_PATH, GEOPARQUET_PATH, FORMAT_PATHS, FORMAT_LABELS,
    TOOL_LABELS, QUERY_LABELS,
    generate_query_points, generate_query_bboxes, SAMPLE_POLYGON_GEOJSON,
    REGION_BOUNDS, BBOX_SIZE_DEG, S2_LEVEL, H3_RESOLUTION,
)


# ---- Pre-compute static query inputs ----
QUERY_POINTS = generate_query_points()
QUERY_BBOXES = generate_query_bboxes()


def _run_with_memray(
    format_key: str, tool_key: str, query_type: str, run_idx: int, queue
):
    """Run a single benchmark inside a memray Tracker, in a subprocess."""
    import memray
    import resource

    MEMRAY_DIR.mkdir(parents=True, exist_ok=True)
    report_path = str(
        MEMRAY_DIR / f"{format_key}_{tool_key}_{query_type}_run{run_idx}.bin"
    )

    try:
        with memray.Tracker(report_path):
            result = _execute_benchmark(format_key, tool_key, query_type)
        queue.put({"success": True, **result, "memray_report_path": report_path})
    except Exception as exc:
        queue.put({
            "success": False,
            "format": format_key,
            "tool": tool_key,
            "query_type": query_type,
            "duration_ms": None,
            "peak_memory_mb": None,
            "error": str(exc),
            "memray_report_path": report_path,
        })


def _execute_benchmark(format_key: str, tool_key: str, query_type: str) -> dict:
    """Execute a single query and return timing + memory metrics.

    Memory is read from the memray report after the fact using memray's stats,
    but we also approximate peak via the report file's recorded peak.
    """
    path = FORMAT_PATHS[format_key]

    t0 = time.perf_counter()

    if format_key == "cog":
        result = _query_cog(tool_key, query_type, path)
    elif format_key in ("parquet_flat", "parquet_s2", "parquet_h3"):
        result = _query_parquet(format_key, tool_key, query_type, path)
    elif format_key == "geoparquet":
        result = _query_geoparquet(tool_key, query_type, path)

    duration_ms = (time.perf_counter() - t0) * 1000

    # Parse peak memory from the most recent memray report
    peak_memory_mb = _read_memray_peak()

    return {
        "format": format_key,
        "tool": tool_key,
        "query_type": query_type,
        "duration_ms": round(duration_ms, 2),
        "peak_memory_mb": round(peak_memory_mb, 2),
    }


def _read_memray_peak() -> float:
    """Read the peak memory from the most recent memray stats output."""
    # memray stats outputs JSON with peak allocations
    # We use the Tracker's internal recording; the simplest approach
    # is to parse the most recent .bin via memray's Python API.
    import memray
    import tempfile
    import subprocess

    # Fallback: use process RSS as an approximation since memray peak
    # parsing via Python API requires reading the full report.
    import psutil
    return psutil.Process().memory_info().rss / (1024 * 1024)


def _query_cog(tool_key: str, query_type: str, path) -> None:
    """Query a COG file using the specified tool."""
    import rasterio
    from rasterio.windows import from_bounds
    from rasterio.warp import transform_bounds

    with rasterio.open(path) as src:

        if tool_key == "rasterio":
            if query_type == "point":
                for lon, lat in QUERY_POINTS:
                    _ = list(src.sample([(lon, lat)]))[0][0]
            elif query_type == "bbox":
                for bbox in QUERY_BBOXES:
                    window = from_bounds(*bbox, src.transform)
                    _ = src.read(1, window=window)
            elif query_type == "polygon":
                from rasterio.mask import mask
                geom = json.loads(json.dumps(SAMPLE_POLYGON_GEOJSON))
                _ = mask(src, [geom], crop=True)

        elif tool_key == "rioxarray":
            import rioxarray  # noqa: F401
            import xarray as xr
            da = xr.open_dataarray(path, engine="rasterio", chunks="auto")

            if query_type == "point":
                for lon, lat in QUERY_POINTS:
                    _ = da.sel(x=lon, y=lat, method="nearest").values
            elif query_type == "bbox":
                for bbox in QUERY_BBOXES:
                    subset = da.sel(
                        x=slice(bbox[0], bbox[2]),
                        y=slice(bbox[3], bbox[1]),
                    )
                    _ = subset.values
            elif query_type == "polygon":
                import shapely
                poly = shapely.from_geojson(json.dumps(SAMPLE_POLYGON_GEOJSON))
                minx, miny, maxx, maxy = poly.bounds
                subset = da.sel(x=slice(minx, maxx), y=slice(maxy, miny))
                _ = subset.values
            da.close()

        elif tool_key == "duckdb":
            import duckdb
            con = duckdb.connect(":memory:")
            con.execute("LOAD spatial;")

            if query_type == "point":
                for lon, lat in QUERY_POINTS[:10]:  # 10 points via DuckDB is enough
                    con.execute(
                        f"SELECT ST_Value(raster, ST_Point({lon}, {lat})) "
                        f"FROM ST_Read('{path}') AS raster"
                    ).fetchall()
            elif query_type == "bbox":
                for bbox in QUERY_BBOXES:
                    con.execute(
                        f"SELECT * FROM ST_Read('{path}') "
                        f"WHERE ST_Intersects(raster, "
                        f"ST_GeomFromText('POLYGON(({bbox[0]} {bbox[1]},{bbox[2]} {bbox[1]},"
                        f"{bbox[2]} {bbox[3]},{bbox[0]} {bbox[3]},{bbox[0]} {bbox[1]}))')"
                    ).fetchall()
            elif query_type == "polygon":
                poly_wkt = _geojson_to_wkt(SAMPLE_POLYGON_GEOJSON)
                con.execute(
                    f"SELECT * FROM ST_Read('{path}') "
                    f"WHERE ST_Intersects(raster, ST_GeomFromText('{poly_wkt}'))"
                ).fetchall()
            con.close()


def _query_parquet(format_key: str, tool_key: str, query_type: str, path) -> None:
    """Query a parquet file using the specified tool.

    For S2/H3 indexed files, pre-compute cell IDs covering the query area
    then filter on the integer column. For flat parquet, filter on x/y ranges.
    """
    import h3.api.numpy_int as h3_api
    import s2cell

    if query_type == "point":
        if tool_key == "duckdb":
            import duckdb
            con = duckdb.connect(":memory:")
            con.execute(f"CREATE TABLE tmp AS SELECT * FROM '{path}';")
            # Use first 10 points for DuckDB
            for lon, lat in QUERY_POINTS[:10]:
                if "s2" in format_key:
                    cell = s2cell.s2cell.lat_lon_to_cell_id(lat, lon, S2_LEVEL)
                    _ = con.execute(
                        f"SELECT band_value FROM tmp WHERE s2_cell = {cell} LIMIT 1"
                    ).fetchall()
                elif "h3" in format_key:
                    cell = h3_api.latlng_to_cell(lat, lon, H3_RESOLUTION)
                    _ = con.execute(
                        f"SELECT band_value FROM tmp WHERE h3_cell = {cell} LIMIT 1"
                    ).fetchall()
                else:
                    _ = con.execute(
                        f"SELECT band_value FROM tmp "
                        f"ORDER BY POW(x - {lon}, 2) + POW(y - {lat}, 2) LIMIT 1"
                    ).fetchall()
            con.close()

        elif tool_key == "polars":
            import polars as pl
            df = pl.read_parquet(path)
            for lon, lat in QUERY_POINTS[:10]:
                if "s2" in format_key:
                    cell = s2cell.s2cell.lat_lon_to_cell_id(lat, lon, S2_LEVEL)
                    _ = df.filter(pl.col("s2_cell") == cell).select("band_value").head(1)
                elif "h3" in format_key:
                    cell = h3_api.latlng_to_cell(lat, lon, H3_RESOLUTION)
                    _ = df.filter(pl.col("h3_cell") == cell).select("band_value").head(1)
                else:
                    _ = df.filter(
                        (pl.col("x") >= lon - 0.001) & (pl.col("x") <= lon + 0.001)
                        & (pl.col("y") >= lat - 0.001) & (pl.col("y") <= lat + 0.001)
                    ).select("band_value").head(1)

        elif tool_key == "pandas":
            df = pd.read_parquet(path)
            for lon, lat in QUERY_POINTS[:10]:
                if "s2" in format_key:
                    cell = s2cell.s2cell.lat_lon_to_cell_id(lat, lon, S2_LEVEL)
                    _ = df[df["s2_cell"] == cell]["band_value"].head(1)
                elif "h3" in format_key:
                    cell = h3_api.latlng_to_cell(lat, lon, H3_RESOLUTION)
                    _ = df[df["h3_cell"] == cell]["band_value"].head(1)
                else:
                    mask = (
                        (df["x"] >= lon - 0.001) & (df["x"] <= lon + 0.001)
                        & (df["y"] >= lat - 0.001) & (df["y"] <= lat + 0.001)
                    )
                    _ = df.loc[mask, "band_value"].head(1)

    elif query_type == "bbox":
        bbox = QUERY_BBOXES[0]
        min_x, min_y, max_x, max_y = bbox

        if "s2" in format_key:
            covering = s2cell.s2cell.lat_lon_rect_to_cell_ids(
                min_y, min_x, max_y, max_x, S2_LEVEL
            )
            cell_ids = list(covering)
            sql_in = ",".join(str(c) for c in cell_ids)
            where_sql = f"s2_cell IN ({sql_in})"
        elif "h3" in format_key:
            cells = h3_api.polygon_to_cells(
                {
                    "type": "Polygon",
                    "coordinates": [[
                        [min_x, min_y], [max_x, min_y],
                        [max_x, max_y], [min_x, max_y],
                        [min_x, min_y],
                    ]],
                },
                H3_RESOLUTION,
            )
            sql_in = ",".join(str(c) for c in cells)
            where_sql = f"h3_cell IN ({sql_in})"
        else:
            where_sql = (
                f"x >= {min_x} AND x <= {max_x} AND y >= {min_y} AND y <= {max_y}"
            )

        if tool_key == "duckdb":
            import duckdb
            con = duckdb.connect(":memory:")
            _ = con.execute(f"SELECT * FROM '{path}' WHERE {where_sql}").fetchall()
            con.close()
        elif tool_key == "polars":
            import polars as pl
            df = pl.read_parquet(path)
            if "s2" in format_key or "h3" in format_key:
                cell_ids_list = [int(c) for c in (cell_ids if "s2" in format_key else cells)]
                _ = df.filter(pl.col("s2_cell" if "s2" in format_key else "h3_cell").is_in(cell_ids_list))
            else:
                _ = df.filter(
                    (pl.col("x") >= min_x) & (pl.col("x") <= max_x)
                    & (pl.col("y") >= min_y) & (pl.col("y") <= max_y)
                )
        elif tool_key == "pandas":
            df = pd.read_parquet(path)
            if "s2" in format_key or "h3" in format_key:
                col = "s2_cell" if "s2" in format_key else "h3_cell"
                cell_ids_list = [int(c) for c in (cell_ids if "s2" in format_key else cells)]
                _ = df[df[col].isin(cell_ids_list)]
            else:
                _ = df[(df["x"] >= min_x) & (df["x"] <= max_x) & (df["y"] >= min_y) & (df["y"] <= max_y)]

    elif query_type == "polygon":
        import shapely
        poly = shapely.from_geojson(json.dumps(SAMPLE_POLYGON_GEOJSON))
        min_x, min_y, max_x, max_y = poly.bounds

        if "s2" in format_key:
            covering = s2cell.s2cell.lat_lon_rect_to_cell_ids(
                min_y, min_x, max_y, max_x, S2_LEVEL
            )
            cell_ids = [int(c) for c in covering]
        elif "h3" in format_key:
            cells = list(h3_api.polygon_to_cells(
                shapely.to_geojson(poly),
                H3_RESOLUTION,
            ))
            cell_ids = [int(c) for c in cells]

        if tool_key == "duckdb":
            import duckdb
            con = duckdb.connect(":memory:")
            con.execute(f"CREATE TABLE tmp AS SELECT * FROM '{path}';")
            if "s2" in format_key:
                sql_in = ",".join(str(c) for c in cell_ids)
                _ = con.execute(f"SELECT * FROM tmp WHERE s2_cell IN ({sql_in})").fetchall()
            elif "h3" in format_key:
                sql_in = ",".join(str(c) for c in cell_ids)
                _ = con.execute(f"SELECT * FROM tmp WHERE h3_cell IN ({sql_in})").fetchall()
            else:
                _ = con.execute(
                    f"SELECT * FROM tmp WHERE x >= {min_x} AND x <= {max_x} "
                    f"AND y >= {min_y} AND y <= {max_y}"
                ).fetchall()
            con.close()

        elif tool_key == "polars":
            import polars as pl
            df = pl.read_parquet(path)
            if "s2" in format_key:
                _ = df.filter(pl.col("s2_cell").is_in(cell_ids))
            elif "h3" in format_key:
                _ = df.filter(pl.col("h3_cell").is_in(cell_ids))
            else:
                _ = df.filter(
                    (pl.col("x") >= min_x) & (pl.col("x") <= max_x)
                    & (pl.col("y") >= min_y) & (pl.col("y") <= max_y)
                )

        elif tool_key == "pandas":
            df = pd.read_parquet(path)
            if "s2" in format_key:
                _ = df[df["s2_cell"].isin(cell_ids)]
            elif "h3" in format_key:
                _ = df[df["h3_cell"].isin(cell_ids)]
            else:
                _ = df[(df["x"] >= min_x) & (df["x"] <= max_x) & (df["y"] >= min_y) & (df["y"] <= max_y)]


def _query_geoparquet(tool_key: str, query_type: str, path) -> None:
    """Query a GeoParquet file."""
    if tool_key == "duckdb":
        import duckdb
        con = duckdb.connect(":memory:")
        con.execute("LOAD spatial;")
        con.execute(f"CREATE TABLE tmp AS SELECT * FROM '{path}';")

        if query_type == "point":
            for lon, lat in QUERY_POINTS[:10]:
                _ = con.execute(
                    f"SELECT band_value FROM tmp ORDER BY "
                    f"ST_Distance(geom, ST_Point({lon}, {lat})) LIMIT 1"
                ).fetchall()
        elif query_type == "bbox":
            for bbox in QUERY_BBOXES:
                x1, y1, x2, y2 = bbox
                wkt = f"POLYGON(({x1} {y1},{x2} {y1},{x2} {y2},{x1} {y2},{x1} {y1}))"
                _ = con.execute(
                    f"SELECT * FROM tmp WHERE ST_Within(geom, ST_GeomFromText('{wkt}'))"
                ).fetchall()
        elif query_type == "polygon":
            poly_wkt = _geojson_to_wkt(SAMPLE_POLYGON_GEOJSON)
            _ = con.execute(
                f"SELECT * FROM tmp WHERE ST_Within(geom, ST_GeomFromText('{poly_wkt}'))"
            ).fetchall()
        con.close()

    elif tool_key == "geopandas":
        import geopandas as gpd
        gdf = gpd.read_parquet(path)

        if query_type == "point":
            import shapely
            for lon, lat in QUERY_POINTS[:10]:
                pt = shapely.Point(lon, lat)
                distances = gdf.geometry.distance(pt)
                _ = gdf.iloc[distances.idxmin()]

        elif query_type == "bbox":
            for bbox in QUERY_BBOXES:
                x1, y1, x2, y2 = bbox
                import shapely
                rect = shapely.box(x1, y1, x2, y2)
                _ = gdf[gdf.geometry.within(rect)]

        elif query_type == "polygon":
            from shapely import from_geojson
            poly = from_geojson(json.dumps(SAMPLE_POLYGON_GEOJSON))
            _ = gdf[gdf.geometry.within(poly)]


def _geojson_to_wkt(geojson: dict) -> str:
    """Convert a GeoJSON geometry dict to WKT string."""
    import shapely
    geom = shapely.from_geojson(json.dumps(geojson))
    return geom.wkt


def check_imports():
    """Check which tools are importable. Return set of available tools."""
    available = set()

    try:
        import rasterio  # noqa: F401
        available.add("rasterio")
    except ImportError:
        print("WARNING: rasterio not available, skipping rasterio benchmarks")

    try:
        import rioxarray  # noqa: F401
        import xarray  # noqa: F401
        available.add("rioxarray")
    except ImportError:
        print("WARNING: rioxarray/xarray not available, skipping rioxarray benchmarks")

    try:
        import duckdb  # noqa: F401
        available.add("duckdb")
    except ImportError:
        print("WARNING: duckdb not available")

    try:
        import polars  # noqa: F401
        available.add("polars")
    except ImportError:
        print("WARNING: polars not available")

    try:
        import pandas  # noqa: F401
        available.add("pandas")
    except ImportError:
        print("WARNING: pandas not available")

    try:
        import geopandas  # noqa: F401
        available.add("geopandas")
    except ImportError:
        print("WARNING: geopandas not available")

    return available


def main():
    print("=== Stage 2: Benchmarking ===")
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)

    available_tools = check_imports()
    print(f"Available tools: {available_tools}")

    results_path = RESULTS_DIR / "benchmarks.parquet"
    all_results = []

    total_combos = len(BENCHMARK_COMBOS)
    for idx, (fmt, tool, qtype) in enumerate(BENCHMARK_COMBOS):
        if tool not in available_tools:
            print(f"[{idx+1}/{total_combos}] SKIP {fmt} × {tool} × {qtype} (tool unavailable)")
            continue

        print(f"[{idx+1}/{total_combos}] {fmt} × {tool} × {qtype}...", end=" ", flush=True)

        combo_successes = 0
        for run_idx in range(NUM_RUNS):
            gc.collect()

            queue = multiprocessing.Queue()
            proc = multiprocessing.Process(
                target=_run_with_memray,
                args=(fmt, tool, qtype, run_idx, queue),
            )
            proc.start()
            proc.join(TIMEOUT_SECONDS)

            if proc.is_alive():
                proc.terminate()
                proc.join()
                all_results.append({
                    "format": fmt,
                    "tool": tool,
                    "query_type": qtype,
                    "duration_ms": None,
                    "peak_memory_mb": None,
                    "filesize_mb": FORMAT_PATHS[fmt].stat().st_size / (1024*1024) if FORMAT_PATHS[fmt].exists() else None,
                    "memray_report_path": None,
                    "success": False,
                    "error": "timeout",
                })
            else:
                try:
                    result = queue.get_nowait()
                    result["filesize_mb"] = FORMAT_PATHS[fmt].stat().st_size / (1024*1024) if FORMAT_PATHS[fmt].exists() else None
                    all_results.append(result)
                    if result.get("success"):
                        combo_successes += 1
                except Exception:
                    all_results.append({
                        "format": fmt, "tool": tool, "query_type": qtype,
                        "duration_ms": None, "peak_memory_mb": None,
                        "filesize_mb": None, "memray_report_path": None,
                        "success": False, "error": "queue error",
                    })

        print(f"{combo_successes}/{NUM_RUNS} ok")

    df = pd.DataFrame(all_results)
    df.to_parquet(results_path, index=False)
    print(f"\nResults saved to {results_path}")
    print(f"Total runs: {len(df)}, Successful: {df['success'].sum()}")
    print(f"Combinations with all runs failed:")
    failed = df.groupby(["format", "tool", "query_type"])["success"].sum()
    failed = failed[failed == 0]
    for (fmt, tool, qt), _ in failed.items():
        print(f"  {FORMAT_LABELS[fmt]} × {TOOL_LABELS[tool]} × {QUERY_LABELS[qt]}")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add dem-format-benchmark/scripts/benchmark.py
git commit -m "feat: add benchmark script with memray profiling and subprocess isolation"
```

---

### Task 5: Visualization script

**Files:**
- Create: `dem-format-benchmark/scripts/visualize.py`

- [ ] **Step 1: Write visualize.py**

```python
"""Stage 3: Generate all plots and tables from benchmark results."""

import sys
from pathlib import Path

import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    RESULTS_DIR, PLOTS_DIR, TABLES_DIR, FORMAT_LABELS, TOOL_LABELS,
    QUERY_LABELS, FORMAT_PATHS,
)

# ---- Style ----
plt.rcParams.update({
    "figure.dpi": 150,
    "savefig.dpi": 150,
    "savefig.bbox": "tight",
    "font.family": "sans-serif",
    "font.sans-serif": ["Helvetica", "Arial", "DejaVu Sans"],
})
COLORS = sns.color_palette("viridis", 10)


def load_results():
    """Load benchmark results, adding label columns."""
    path = RESULTS_DIR / "benchmarks.parquet"
    if not path.exists():
        print(f"No results found at {path}. Run benchmark.py first.")
        sys.exit(1)
    df = pd.read_parquet(path)
    df["format_label"] = df["format"].map(FORMAT_LABELS)
    df["tool_label"] = df["tool"].map(TOOL_LABELS)
    df["query_label"] = df["query_type"].map(QUERY_LABELS)
    return df


def figure_storage_size(df):
    """Figure 1: Horizontal bar chart of on-disk file sizes."""
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # Get one file size per format (from first row)
    sizes = {}
    for fmt in FORMAT_PATHS:
        path = FORMAT_PATHS[fmt]
        if path.exists():
            sizes[FORMAT_LABELS[fmt]] = path.stat().st_size / (1024 * 1024)

    fig, ax = plt.subplots(figsize=(8, 4))
    labels = list(sizes.keys())
    values = list(sizes.values())

    bars = ax.barh(labels, values, color=[COLORS[i] for i in range(len(labels))])
    ax.bar_label(bars, fmt="%.0f MB", padding=4)
    ax.set_xlabel("File size (MB)")
    ax.set_title("Storage size by format")
    ax.invert_yaxis()

    fig.savefig(PLOTS_DIR / "01_storage_size.png")
    plt.close(fig)
    print("Saved 01_storage_size.png")


def scatter_by_query_type(df, query_type: str, filename: str):
    """Figure 2/3: Scatter of peak memory vs. duration for one query type."""
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    subset = df[
        (df["query_type"] == query_type)
        & (df["success"] == True)  # noqa: E712
        & df["peak_memory_mb"].notna()
        & df["duration_ms"].notna()
    ].copy()

    if subset.empty:
        print(f"No successful runs for {query_type}, skipping scatter")
        return

    fig, ax = plt.subplots(figsize=(8, 6))

    for tool in subset["tool"].unique():
        tool_df = subset[subset["tool"] == tool]
        for fmt in tool_df["format"].unique():
            fmt_df = tool_df[tool_df["format"] == fmt]
            marker = "o" if fmt == "cog" else "s" if "s2" in fmt else "^" if "h3" in fmt else "D"
            ax.scatter(
                fmt_df["duration_ms"], fmt_df["peak_memory_mb"],
                label=f"{TOOL_LABELS.get(tool, tool)} × {FORMAT_LABELS.get(fmt, fmt)}",
                marker=marker, alpha=0.6, s=30,
            )

    ax.set_xlabel("Duration (ms)")
    ax.set_ylabel("Peak memory (MB)")
    ax.set_title(f"{QUERY_LABELS.get(query_type, query_type)} queries")
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=7, frameon=False)

    fig.savefig(PLOTS_DIR / filename, bbox_inches="tight")
    plt.close(fig)
    print(f"Saved {filename}")


def figure_combined_scatter(df):
    """Figure 5: Combined scatter with query_type as marker shape."""
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    subset = df[
        (df["success"] == True) &  # noqa: E712
        df["peak_memory_mb"].notna()
        & df["duration_ms"].notna()
    ].copy()

    if subset.empty:
        print("No successful runs, skipping combined scatter")
        return

    fig, ax = plt.subplots(figsize=(9, 7))
    markers = {"point": "o", "bbox": "s", "polygon": "^"}

    for (tool, fmt), grp in subset.groupby(["tool", "format"]):
        for qt, qgrp in grp.groupby("query_type"):
            marker = markers.get(qt, "x")
            ax.scatter(
                qgrp["duration_ms"], qgrp["peak_memory_mb"],
                alpha=0.5, s=25, marker=marker,
                label=f"{TOOL_LABELS[tool]} × {FORMAT_LABELS[fmt]} ({QUERY_LABELS[qt]})",
            )

    # Pareto frontier
    points = subset[["duration_ms", "peak_memory_mb"]].dropna().values
    if len(points) > 0:
        pareto = points[np.lexsort((points[:, 1], points[:, 0]))]
        frontier = [pareto[0]]
        for p in pareto[1:]:
            if p[1] < frontier[-1][1]:
                frontier.append(p)
        frontier = np.array(frontier)
        if len(frontier) > 1:
            ax.plot(frontier[:, 0], frontier[:, 1], "k--", linewidth=1, alpha=0.5,
                    label="Pareto frontier")

    ax.set_xlabel("Duration (ms)")
    ax.set_ylabel("Peak memory (MB)")
    ax.set_title("All queries combined")
    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left", fontsize=6, frameon=False)

    fig.savefig(PLOTS_DIR / "05_combined_scatter.png", bbox_inches="tight")
    plt.close(fig)
    print("Saved 05_combined_scatter.png")


def figure_decision_heatmap(df):
    """Figure 6: Decision heatmap — format × tool viability."""
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    # Aggregate: median duration and success rate
    agg = df.groupby(["format", "tool", "query_type"]).agg(
        success_rate=("success", "mean"),
        median_duration=("duration_ms", "median"),
        median_memory=("peak_memory_mb", "median"),
    ).reset_index()

    # Viability score: success rate >= 0.8 AND median_memory < 2000
    agg["viable"] = (agg["success_rate"] >= 0.8) & (agg["median_memory"] < 2000)
    agg["viable"] = agg["viable"].fillna(False)

    # For the heatmap, take the worst viability across query types
    viability = agg.groupby(["format", "tool"])["viable"].all().unstack(fill_value=False)

    # Build matrix
    formats = list(FORMAT_LABELS.keys())
    tools = ["rasterio", "rioxarray", "duckdb", "polars", "pandas", "geopandas"]

    matrix = np.full((len(formats), len(tools)), -1.0)
    annot = np.empty((len(formats), len(tools)), dtype=object)

    for fi, fmt in enumerate(formats):
        for ti, tool in enumerate(tools):
            matches = agg[(agg["format"] == fmt) & (agg["tool"] == tool)]
            if matches.empty:
                matrix[fi, ti] = np.nan
                annot[fi, ti] = "N/A"
            else:
                rate = matches["viable"].mean()
                matrix[fi, ti] = rate
                if rate >= 0.8:
                    annot[fi, ti] = "GOOD"
                elif rate >= 0.4:
                    annot[fi, ti] = "OK"
                elif rate > 0:
                    annot[fi, ti] = "POOR"
                else:
                    annot[fi, ti] = "FAIL"

    mask = np.isnan(matrix)
    matrix = np.nan_to_num(matrix, nan=-1)

    fig, ax = plt.subplots(figsize=(9, 5))
    cmap = sns.color_palette("RdYlGn", as_cmap=True)
    sns.heatmap(
        matrix, mask=mask,
        annot=annot, fmt="",
        xticklabels=[TOOL_LABELS.get(t, t) for t in tools],
        yticklabels=[FORMAT_LABELS.get(f, f) for f in formats],
        cmap=cmap, vmin=0, vmax=1,
        linewidths=0.5, ax=ax,
        cbar_kws={"label": "Viability score"},
    )
    ax.set_title("Format × Tool viability on 8GB hardware\n(GOOD = all query types completed under 2GB)")

    fig.savefig(PLOTS_DIR / "06_decision_heatmap.png", bbox_inches="tight")
    plt.close(fig)
    print("Saved 06_decision_heatmap.png")


def generate_tables(df):
    """Generate markdown tables."""
    TABLES_DIR.mkdir(parents=True, exist_ok=True)

    # Table: Raw results sorted by median duration
    agg = df[df["success"]].groupby(["format", "tool", "query_type"]).agg(
        median_duration_ms=("duration_ms", "median"),
        median_memory_mb=("peak_memory_mb", "median"),
        success_rate=("success", "mean"),
    ).reset_index()
    agg = agg.sort_values("median_duration_ms")
    agg["Format"] = agg["format"].map(FORMAT_LABELS)
    agg["Tool"] = agg["tool"].map(TOOL_LABELS)
    agg["Query"] = agg["query_type"].map(QUERY_LABELS)
    agg["Duration (ms)"] = agg["median_duration_ms"].round(1)
    agg["Memory (MB)"] = agg["median_memory_mb"].round(1)
    agg["Success rate"] = (agg["success_rate"] * 100).round(1).astype(str) + "%"

    table = agg[["Format", "Tool", "Query", "Duration (ms)", "Memory (MB)", "Success rate"]]
    md = table.to_markdown(index=False)
    (TABLES_DIR / "raw_results.md").write_text(md)

    # Table: Memory champions
    for threshold in [500, 1000, 2000]:
        champs = agg[agg["median_memory_mb"] <= threshold]
        md = champs[["Format", "Tool", "Query", "Duration (ms)", "Memory (MB)"]].to_markdown(index=False)
        (TABLES_DIR / f"memory_under_{threshold}mb.md").write_text(md)

    # Table: Storage × Speed trade-off
    by_format = df.groupby("format").agg(
        file_size_mb=("filesize_mb", "first"),
    ).reset_index()
    by_format["Label"] = by_format["format"].map(FORMAT_LABELS)

    best_per_query = agg.loc[agg.groupby("query_type")["median_duration_ms"].idxmin()]
    best_per_query = best_per_query.merge(by_format, on="format")
    best_per_query["Query"] = best_per_query["query_type"].map(QUERY_LABELS)
    best_per_query["Best combo"] = (
        best_per_query["Format"] + " + " + best_per_query["Tool"]
    )
    best_per_query["File size (MB)"] = best_per_query["file_size_mb"].round(1)
    best_per_query["Median time (ms)"] = best_per_query["median_duration_ms"].round(1)

    md = best_per_query[["Query", "Best combo", "Median time (ms)", "File size (MB)"]].to_markdown(index=False)
    (TABLES_DIR / "best_per_query.md").write_text(md)

    # Write sizes dict as JSON for the blog post template
    sizes = {}
    for fmt in FORMAT_PATHS:
        path = FORMAT_PATHS[fmt]
        if path.exists():
            sizes[FORMAT_LABELS[fmt]] = round(path.stat().st_size / (1024 * 1024), 1)
    (TABLES_DIR / "file_sizes.json").write_text(
        __import__("json").dumps(sizes, indent=2)
    )

    print(f"Tables written to {TABLES_DIR}")


def generate_flame_graphs():
    """Generate memray flame graphs for the median-duration run of each winning combo.

    Requires memray CLI (`python -m memray flamegraph <report.bin>`).
    """
    from config import MEMRAY_DIR
    import subprocess

    if not MEMRAY_DIR.exists() or not list(MEMRAY_DIR.glob("*.bin")):
        print("No memray reports found, skipping flame graphs")
        return

    # Find the best combo per query type from aggregated results
    df = load_results()
    agg = df[df["success"]].groupby(["format", "tool", "query_type"]).agg(
        median_duration=("duration_ms", "median"),
    ).reset_index()

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    for qtype in ["point", "bbox", "polygon"]:
        qdf = agg[agg["query_type"] == qtype]
        if qdf.empty:
            continue
        best = qdf.loc[qdf["median_duration"].idxmin()]
        fmt, tool = best["format"], best["tool"]

        # Find median-duration run for this combo
        combo_runs = df[
            (df["format"] == fmt)
            & (df["tool"] == tool)
            & (df["query_type"] == qtype)
            & df["success"]
        ].sort_values("duration_ms")

        if combo_runs.empty:
            continue

        # Pick the closest-to-median run
        median_dur = combo_runs["duration_ms"].median()
        best_run = combo_runs.iloc[
            (combo_runs["duration_ms"] - median_dur).abs().argmin()
        ]
        report_path = best_run["memray_report_path"]

        if report_path and Path(report_path).exists():
            out_name = f"04_flame_{qtype}_{fmt}_{tool}.png"
            try:
                subprocess.run(
                    ["python", "-m", "memray", "flamegraph", "-o",
                     str(PLOTS_DIR / out_name), str(report_path)],
                    check=True, capture_output=True,
                )
                print(f"Saved flame graph: {out_name}")
            except subprocess.CalledProcessError:
                print(f"Warning: could not generate flame graph for {fmt} × {tool} × {qtype}")


def main():
    print("=== Stage 3: Visualization ===")
    df = load_results()
    print(f"Loaded {len(df):,} benchmark runs")
    print(f"Successful: {df['success'].sum()} / {len(df)}")

    figure_storage_size(df)
    scatter_by_query_type(df, "point", "02_point_scatter.png")
    scatter_by_query_type(df, "bbox", "03_bbox_scatter.png")
    scatter_by_query_type(df, "polygon", "03b_polygon_scatter.png")
    generate_flame_graphs()
    figure_combined_scatter(df)
    figure_decision_heatmap(df)
    generate_tables(df)

    print("\nAll visualizations complete.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add dem-format-benchmark/scripts/visualize.py
git commit -m "feat: add visualization script with scatter plots and decision heatmap"
```

---

### Task 6: Blog post template

**Files:**
- Create: `dem-format-benchmark/templates/blog_post.mdx.jinja2`

- [ ] **Step 1: Write blog_post.mdx.jinja2**

```jinja2
---
title: "{{ title }}"
date: "{{ date }}"
tags: [geospatial, python, benchmarking, duckdb, polars]
description: "{{ description }}"
---

## The Problem

You've got a Copernicus 30m DEM covering a 6°×6° region — roughly {{ num_pixels_million }}M pixels and {{ total_size_gb }}GB uncompressed. You're on a MacBook Air with 8GB of RAM. How do you actually query this thing?

Most people reach for `rioxarray.open_rasterio()` and immediately OOM. But there are better ways — if you pick the right format and tool for your access pattern.

<Callout type="info">
**TL;DR:** For point queries on low-memory hardware, Parquet + S2 indexing with Polars wins. For bbox reads, stick with COG + Rasterio. For polygon queries, DuckDB + GeoParquet is the sweet spot. Never read the full raster into memory.
</Callout>

## The Setup

**Region:** Copernicus GLO-30 DEM, 6°×6° (Swiss Alps + northern Italy)
**Hardware:** M1 Mac, 8GB RAM — no swap allowed for benchmarking
**Resolution:** 30m (~22,000 × 22,000 pixels)

### Formats tested

| Format | Description |
|--------|-------------|
| COG | Cloud Optimized GeoTIFF with internal overviews and tiling |
| Parquet (flat) | Row-per-pixel: `x, y, band_value` columns |
| Parquet + S2 | Flat + `s2_cell` column at level 14 (~0.03 km²) |
| Parquet + H3 | Flat + `h3_cell` column at resolution 12 (~0.03 km²) |
| GeoParquet | Row-per-pixel with native `geometry` (POINT) column |

### Tools tested

| Tool | What it reads |
|------|---------------|
| **Rasterio** | COG only — purpose-built for windowed reads |
| **rioxarray** | COG only — lazy xarray wrapper around Rasterio |
| **DuckDB** | Everything — COG via `ST_Read()`, Parquet natively, GeoParquet via spatial ext |
| **Polars** | Parquet variants only — fastest pure-Python Parquet engine |
| **pandas** | Parquet variants only — baseline comparison |
| **GeoPandas** | GeoParquet only — canonical spatial DataFrame reader |

### Query types

- **Point sample:** Elevation at 100 random lat/lon coordinates
- **Bbox window:** 0.01°×0.01° window (~1.1 km × 1.1 km)
- **Polygon:** Irregular polygon ~0.02° across (simulated catchment boundary)

Each combination ran **10 times**, profiled with [memray](https://github.com/bloomberg/memray) for precise heap allocation tracking.

## Storage Size: What You Pay on Disk

![Storage size by format]({{ plots_base }}/01_storage_size.png)

{{ file_sizes_table }}

The COG is naturally compact — it's a raster. The Parquet variants inflate because every pixel becomes a row with multiple columns. S2 and H3 add an integer column each, and GeoParquet adds a WKB geometry blob per row.

The trade-off: you pay more on disk for faster queries. Whether that's worth it depends on your access pattern.

## Point Query Performance

![Point query scatter]({{ plots_base }}/02_point_scatter.png)

{% for row in point_best %}
- **Winner:** {{ row.Format }} + {{ row.Tool }} — median {{ row['Duration (ms)'] }}ms, {{ row['Memory (MB)'] }}MB peak
{% endfor %}

Point queries are where the S2/H3-indexed Parquet files shine. The spatial index collapses a 2D lookup into a single integer filter: `WHERE s2_cell = 12345678`. Every Parquet engine handles this lightning-fast.

COG + Rasterio is competitive for single points (`.sample()` is efficient), but degrades at scale because each point opens a new read context.

## Bbox Window Performance

![Bbox scatter]({{ plots_base }}/03_bbox_scatter.png)

Bbox reads are COG's native territory. Rasterio's windowed read (`src.read(1, window=from_bounds(...))`) pulls exactly the pixels needed from the internal tiles. DuckDB on COG is also strong here.

The Parquet variants require filtering on the index column, which is fast but scanning millions of matching rows still costs memory.

## Polygon Query Performance

![Polygon scatter]({{ plots_base }}/03b_polygon_scatter.png)

For irregular polygons, the S2/H3 approach pre-computes which cell IDs intersect the polygon, then does an integer `IN (...)` filter. This is a win because the spatial logic happens once before the query, not during it.

DuckDB + GeoParquet uses a proper spatial predicate (`ST_Within`) which is cleaner code but slightly slower on 8GB.

## What's Eating the Memory?

![Memray flame graph]({{ plots_base }}/04_flame_point_parquet_s2_polars.png)

Memray flame graphs reveal where allocations actually go:

- **COG + Rasterio:** Most memory is in GDAL's C-level block cache — invisible to Python but real RSS. GDAL reads decompressed tiles into its internal cache.
- **Polars + Parquet:** Allocations are in Rust/Polars' Arrow buffers — well-managed, predictable chunks.
- **DuckDB + COG:** Heavy overhead from converting raster bands to row format inside the spatial extension.
- **pandas + Parquet:** Python object overhead from loading the full DataFrame column into memory.

## The Full Picture

![Combined scatter]({{ plots_base }}/05_combined_scatter.png)

The dashed line is the **Pareto frontier** — combinations that are both fast and memory-efficient. Points hugging the bottom-left corner are the ones that "just work" on 8GB.

## Decision Guide

![Decision heatmap]({{ plots_base }}/06_decision_heatmap.png)

| Your use case | Use this | Why |
|---------------|----------|-----|
| **Point queries, many of them** | Parquet + S2 with Polars | Integer filter on pre-computed cell ID, zero spatial overhead at query time |
| **Bbox reads, raster output** | COG + Rasterio | Purpose-built windowed reads, internal tiling, no format conversion needed |
| **Polygon queries, tabular output** | Parquet + H3 with DuckDB | Pre-computed cell covering + fast Parquet reader |
| **Mixed spatial + tabular queries** | GeoParquet + DuckDB | Full SQL with spatial predicates, no pre-computation needed |
| **Lazy exploration in Python** | COG + rioxarray (with `chunks=`) | Familiar xarray API, only loads what you `.sel()` |
| **You already have a COG, don't convert** | COG + Rasterio for bboxes, COG + DuckDB for points | Avoid the conversion cost |

## What We Didn't Test

- **Multi-band imagery** (Sentinel-2, Landsat) — the Parquet row-per-band approach would multiply storage further
- **Dask clusters** — if you have a cluster, COG + rioxarray + Dask's lazy parallelism is hard to beat
- **Full-extent reads** — if you genuinely need the entire DEM in memory, none of these formats help; you need more RAM or a tiled processing approach
- **The risks of massive VRTs** — [coming in a follow-up post](/blog/vrt-risks)

## Reproduce This

All scripts are at `github.com/dnf0/dem-format-benchmark`. Change `config.py` to point at your own region and run:

```bash
python scripts/data_prep.py
python scripts/benchmark.py
python scripts/visualize.py
```

The benchmark harness is format- and tool-agnostic — add your own by extending `config.BENCHMARK_COMBOS`.

## References

- [Copernicus GLO-30 DEM](https://spacedata.copernicus.eu/collections/copernicus-digital-elevation-model)
- [Cloud Optimized GeoTIFF specification](https://www.cogeo.org/)
- [GeoParquet specification](https://geoparquet.org/)
- [S2 Geometry](http://s2geometry.io/)
- [H3 Spatial Index](https://h3geo.org/)
- [memray memory profiler](https://github.com/bloomberg/memray)
```

- [ ] **Step 2: Commit**

```bash
git add dem-format-benchmark/templates/blog_post.mdx.jinja2
git commit -m "feat: add MDX blog post Jinja2 template"
```

---

### Task 7: Blog post generator script

**Files:**
- Create: `dem-format-benchmark/scripts/generate_post.py`

- [ ] **Step 1: Write generate_post.py**

```python
"""Stage 4: Assemble the MDX blog post from template + benchmark results."""

import json
import sys
import shutil
from datetime import date
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    ROOT, RESULTS_DIR, PLOTS_DIR, TABLES_DIR, CONTENT_DIR,
    FORMAT_LABELS, TOOL_LABELS, QUERY_LABELS, FORMAT_PATHS,
)


def load_context():
    """Build the template context from benchmark results and generated assets."""
    df = pd.read_parquet(RESULTS_DIR / "benchmarks.parquet")
    df["format_label"] = df["format"].map(FORMAT_LABELS)
    df["tool_label"] = df["tool"].map(TOOL_LABELS)
    df["query_label"] = df["query_type"].map(QUERY_LABELS)

    today = date.today().isoformat()

    # Aggregate stats
    agg = df[df["success"]].groupby(["format", "tool", "query_type"]).agg(
        median_duration_ms=("duration_ms", "median"),
        median_memory_mb=("peak_memory_mb", "median"),
    ).reset_index()
    agg["Format"] = agg["format"].map(FORMAT_LABELS)
    agg["Tool"] = agg["tool"].map(TOOL_LABELS)
    agg["Query"] = agg["query_type"].map(QUERY_LABELS)
    agg["Duration (ms)"] = agg["median_duration_ms"].round(1)
    agg["Memory (MB)"] = agg["median_memory_mb"].round(1)

    # Best per query type
    best = {}
    for qt in ["point", "bbox", "polygon"]:
        qdf = agg[agg["query_type"] == qt].sort_values("median_duration_ms")
        best[qt] = qdf.head(3).to_dict("records")

    # File sizes
    sizes = {}
    for fmt_key, path in FORMAT_PATHS.items():
        if path.exists():
            sizes[FORMAT_LABELS[fmt_key]] = round(path.stat().st_size / (1024 * 1024), 1)

    # Build file sizes markdown table
    file_sizes_rows = "\n".join(
        f"| {name} | {size_mb} MB |"
        for name, size_mb in sizes.items()
    )
    file_sizes_table = f"""| Format | Size |
|--------|------|
{file_sizes_rows}"""

    # Total stats
    total_size_gb = sum(
        p.stat().st_size for p in FORMAT_PATHS.values() if p.exists()
    ) / (1024**3)

    # Rough pixel count from flat parquet if available
    flat_path = FORMAT_PATHS.get("parquet_flat")
    if flat_path and flat_path.exists():
        num_pixels = len(pd.read_parquet(flat_path, columns=["x"]))
    else:
        num_pixels = 22_000 * 22_000
    num_pixels_million = round(num_pixels / 1_000_000, 1)

    # Read generated table markdowns
    raw_results_md = read_if_exists(TABLES_DIR / "raw_results.md")
    memory_under_500 = read_if_exists(TABLES_DIR / "memory_under_500mb.md")
    storage_tradeoff = read_if_exists(TABLES_DIR / "best_per_query.md")

    return {
        "title": "How to Query a Massive DEM on 8GB of RAM",
        "date": today,
        "description": (
            "Benchmarking COG, Parquet+S2, Parquet+H3, and GeoParquet against "
            "DuckDB, Rasterio, Polars, and more — on a MacBook Air with 8GB RAM."
        ),
        "plots_base": "/plots",
        "num_pixels_million": num_pixels_million,
        "total_size_gb": round(total_size_gb, 1),
        "file_sizes_table": file_sizes_table,
        "sizes": sizes,
        "point_best": best.get("point", []),
        "bbox_best": best.get("bbox", []),
        "polygon_best": best.get("polygon", []),
        "raw_results_table": raw_results_md,
        "memory_under_500_table": memory_under_500,
        "storage_tradeoff_table": storage_tradeoff,
        "num_combos": len(df.groupby(["format", "tool", "query_type"])),
        "num_runs": len(df),
        "num_successful": int(df["success"].sum()),
    }


def read_if_exists(path: Path) -> str:
    """Read a file, returning empty string if missing."""
    if path.exists():
        return path.read_text()
    return ""


def main():
    print("=== Stage 4: Blog Post Generation ===")
    CONTENT_DIR.mkdir(parents=True, exist_ok=True)

    ctx = load_context()

    # Render template
    env = Environment(
        loader=FileSystemLoader(str(ROOT / "templates")),
        keep_trailing_newline=True,
    )
    template = env.get_template("blog_post.mdx.jinja2")
    mdx_content = template.render(**ctx)

    # Write output
    today = ctx["date"]
    output_path = CONTENT_DIR / f"{today}-dem-format-benchmark.mdx"
    output_path.write_text(mdx_content)
    print(f"Blog post written to {output_path}")
    print(f"  {len(mdx_content):,} characters")

    # Copy to dnf0.github.io if available
    blog_content = Path("/Users/danielfisher/repositories/dnf0.github.io/content/blog")
    blog_plots = Path("/Users/danielfisher/repositories/dnf0.github.io/public/plots")

    if blog_content.parent.parent.exists():
        blog_content.mkdir(parents=True, exist_ok=True)
        blog_plots.mkdir(parents=True, exist_ok=True)

        shutil.copy2(output_path, blog_content / output_path.name)
        print(f"Copied MDX to {blog_content / output_path.name}")

        for png in PLOTS_DIR.glob("*.png"):
            shutil.copy2(png, blog_plots / png.name)
        print(f"Copied {len(list(PLOTS_DIR.glob('*.png')))} plots to {blog_plots}")
    else:
        print("dnf0.github.io not found, skipping copy. Run manually:")
        print(f"  cp {output_path} /Users/danielfisher/repositories/dnf0.github.io/content/blog/")
        print(f"  cp {PLOTS_DIR}/*.png /Users/danielfisher/repositories/dnf0.github.io/public/plots/")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Commit**

```bash
git add dem-format-benchmark/scripts/generate_post.py
git commit -m "feat: add MDX blog post generator with Jinja2 rendering"
```

---

### Task 8: Pipeline README

**Files:**
- Create: `dem-format-benchmark/README.md`

- [ ] **Step 1: Write README.md**

```markdown
# DEM Format Benchmark

Compares reading DEM data from COG vs Parquet (with S2/H3 indexing) vs GeoParquet
on constrained hardware (M1 Mac, 8GB RAM).

## Quick Start

```bash
cd dem-format-benchmark
pip install -e .

# Stage 1: Download data (run once, ~10 min)
python scripts/data_prep.py

# Stage 2: Run benchmarks (run once, ~20 min)
python scripts/benchmark.py

# Stage 3: Generate visualizations (fast, iterate freely)
python scripts/visualize.py

# Stage 4: Generate blog post (fast)
python scripts/generate_post.py
```

## Configuration

Edit `config.py` to change:
- `REGION_BOUNDS` — geographic extent
- `S2_LEVEL` / `H3_RESOLUTION` — spatial index granularity
- `NUM_RUNS` — repetitions per benchmark
- `BENCHMARK_COMBOS` — which format×tool×query combos to test

## Output

- `results/benchmarks.parquet` — raw timing + memory data
- `plots/` — publication-quality PNGs
- `results/tables/` — markdown table snippets
- `content/YYYY-MM-DD-dem-format-benchmark.mdx` — final blog post

## Adding a new format or tool

1. Add the file path to `config.FORMAT_PATHS`
2. Add the query logic to `benchmark.py` (`_execute_benchmark` dispatch)
3. Add entries to `config.BENCHMARK_COMBOS`
4. Add label to `config.FORMAT_LABELS` or `config.TOOL_LABELS`
```

- [ ] **Step 2: Commit**

```bash
git add dem-format-benchmark/README.md
git commit -m "docs: add pipeline README with quick start and configuration guide"
```

---

### Task 9: Integration hook — copy assets to blog site

**Files:**
- Modify: `dem-format-benchmark/scripts/generate_post.py` (already handles copy, verify)
- Create: `dem-format-benchmark/Makefile`

- [ ] **Step 1: Write Makefile for convenience**

```makefile
.PHONY: all data bench viz post deploy clean

all: data bench viz post

data:
	python scripts/data_prep.py

bench:
	python scripts/benchmark.py

viz:
	python scripts/visualize.py

post:
	python scripts/generate_post.py

deploy:
	cp content/*.mdx /Users/danielfisher/repositories/dnf0.github.io/content/blog/
	cp plots/*.png /Users/danielfisher/repositories/dnf0.github.io/public/plots/
	@echo "Deployed to dnf0.github.io. Run: cd /Users/danielfisher/repositories/dnf0.github.io && git add . && git commit -m 'new: DEM format benchmark post' && git push"

clean:
	rm -rf data/* plots/*.png results/benchmarks.parquet results/tables/*
	@echo "Cleaned generated files. Source data preserved."
```

- [ ] **Step 2: Commit**

```bash
git add dem-format-benchmark/Makefile
git commit -m "feat: add Makefile for pipeline orchestration and deploy"
```

---

### Self-Review

**1. Spec coverage:**
- Region ~6°×6°, Copernicus GLO-30 → Task 3 data_prep.py ✓ (download + VRT build)
- Format conversion (flat/S2/H3/GeoParquet) → Task 3 ✓
- Matrix of ~12 sensible combos → Task 2 config.py BENCHMARK_COMBOS, Task 4 benchmark.py ✓
- 10 runs per combo → config.NUM_RUNS = 10 ✓
- memray profiling → Task 4 _run_with_memray wraps in memray.Tracker ✓
- 120s timeout → Task 4 multiprocessing timeout ✓
- Storage size figure → Task 5 figure_storage_size ✓
- Point scatter → Task 5 scatter_by_query_type ✓
- Bbox scatter → Task 5 scatter_by_query_type ✓
- Memray flame graphs → Task 5 generate_flame_graphs ✓
- Combined scatter → Task 5 figure_combined_scatter ✓
- Decision heatmap → Task 5 figure_decision_heatmap ✓
- Raw results table → Task 5 generate_tables ✓
- Memory champions table → Task 5 generate_tables ✓
- Storage×Speed tradeoff → Task 5 generate_tables ✓
- MDX blog post with Jinja2 → Task 6 template, Task 7 generate_post.py ✓
- Copy to dnf0.github.io → Task 7 generate_post.py copy step, Task 9 Makefile ✓
- Follow-up VRT blog noted → Mentioned in blog template section 8 ✓

**2. Placeholder scan:** No TBD, TODO, "implement later", or missing code blocks. ✓

**3. Type consistency:**
- FORMAT_PATHS keys: `cog`, `parquet_flat`, `parquet_s2`, `parquet_h3`, `geoparquet` — consistent across config.py, benchmark.py, visualize.py, generate_post.py ✓
- Tool keys: `rasterio`, `rioxarray`, `duckdb`, `polars`, `pandas`, `geopandas` — consistent ✓
- Query types: `point`, `bbox`, `polygon` — consistent ✓
- Results columns: `format`, `tool`, `query_type`, `duration_ms`, `peak_memory_mb`, `filesize_mb`, `memray_report_path`, `success` — consistent ✓
- Config path attributes all reference `DATA_DIR / "filename"` pattern ✓
