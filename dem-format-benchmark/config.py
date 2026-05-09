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
