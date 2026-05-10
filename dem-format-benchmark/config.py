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

# ---- Region (Swiss Alps + northern Italy, ~6 x 6) ----
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
S2_LEVEL = 14        # ~0.03 km per cell
H3_RESOLUTION = 12   # ~0.03 km mean area

# ---- Benchmark params ----
NUM_RUNS = 10
TIMEOUT_SECONDS = 30

# ---- Bbox size for spatial queries ----
BBOX_SIZE_DEG = 0.01   # ~1.1 km at these latitudes

# ---- Polygon for area query (irregular, ~0.02 across) ----
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

# ---- Smoothing variants ----
SMOOTHING_SIGMAS = [3, 15, 21]
DATA_VARIANTS = ["raw", "s3", "s15", "s21", "m10", "m20", "m50", "m100"]


def _build_paths():
    """Build nested FORMAT_PATHS: {format_key: {variant: Path}}."""
    paths = {}
    for tag in DATA_VARIANTS:
        suffix = f"_{tag}" if tag != "raw" else ""
        paths[tag] = {
            "cog":             DATA_DIR / f"dem_cog{suffix}.tif",
            "parquet_flat":    DATA_DIR / f"dem_flat{suffix}.parquet",
            "parquet_s2":      DATA_DIR / f"dem_s2{suffix}.parquet",
            "parquet_h3":      DATA_DIR / f"dem_h3{suffix}.parquet",
            "geoparquet":      DATA_DIR / f"dem{suffix}.geoparquet",
        }

    # Flatten to FORMAT_PATHS[format][variant]
    result = {}
    for fmt_key in ["cog", "parquet_flat", "parquet_s2", "parquet_h3", "geoparquet"]:
        result[fmt_key] = {tag: paths[tag][fmt_key] for tag in DATA_VARIANTS}
    return result


FORMAT_PATHS = _build_paths()


def get_path(format_key: str, variant: str = "raw") -> Path:
    """Return the file path for a format + smoothing variant."""
    return FORMAT_PATHS[format_key][variant]


# Backwards-compatible aliases
COG_PATH = FORMAT_PATHS["cog"]["raw"]
FLAT_PARQUET_PATH = FORMAT_PATHS["parquet_flat"]["raw"]
S2_PARQUET_PATH = FORMAT_PATHS["parquet_s2"]["raw"]
H3_PARQUET_PATH = FORMAT_PATHS["parquet_h3"]["raw"]
GEOPARQUET_PATH = FORMAT_PATHS["geoparquet"]["raw"]

# ---- Format x tool combinations to benchmark ----
# Each tuple: (format_key, tool_key, query_type)
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
    # Parquet + S2
    ("parquet_s2", "duckdb", "point"),
    ("parquet_s2", "duckdb", "bbox"),
    ("parquet_s2", "duckdb", "polygon"),
    ("parquet_s2", "polars", "point"),
    ("parquet_s2", "polars", "bbox"),
    ("parquet_s2", "polars", "polygon"),
    # Parquet + H3
    ("parquet_h3", "duckdb", "point"),
    ("parquet_h3", "duckdb", "bbox"),
    ("parquet_h3", "duckdb", "polygon"),
    ("parquet_h3", "polars", "point"),
    ("parquet_h3", "polars", "bbox"),
    ("parquet_h3", "polars", "polygon"),
    # GeoParquet
    ("geoparquet", "duckdb", "point"),
    ("geoparquet", "duckdb", "bbox"),
    ("geoparquet", "duckdb", "polygon"),
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
    "geopandas": "GeoPandas",
}

QUERY_LABELS = {
    "point": "Point sample",
    "bbox": "Bbox window",
    "polygon": "Polygon",
}

VARIANT_LABELS = {
    "raw": "Raw",
    "s3": "Smoothed (=3)",
    "s15": "Smoothed (=15)",
    "s21": "Smoothed (=21)",
    "m10": "Median Filter (size=10)",
    "m20": "Median Filter (size=20)",
    "m50": "Median Filter (size=50)",
    "m100": "Median Filter (size=100)",
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
