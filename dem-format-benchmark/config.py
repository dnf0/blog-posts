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
S2_LEVEL = 19        # ~18m per cell (Polygon Covering ensures no holes)
H3_RESOLUTION = 14   # ~15m mean area (Nyquist limit for 30m DEM)

# ---- Benchmark params ----
NUM_RUNS = 3
TIMEOUT_SECONDS = 30

# ---- Bbox size for spatial queries ----
BBOX_SIZE_DEG = 0.01   # ~1.1 km at these latitudes

def generate_query_polygons(n=10, seed=SEED):
    """Generate reproducible random polygons within the region."""
    import numpy as np
    rng = np.random.default_rng(seed)
    polygons = []
    for _ in range(n):
        lon = rng.uniform(
            REGION_BOUNDS["min_lon"], REGION_BOUNDS["max_lon"] - 0.02
        )
        lat = rng.uniform(
            REGION_BOUNDS["min_lat"], REGION_BOUNDS["max_lat"] - 0.02
        )
        polygons.append({
            "type": "Polygon",
            "coordinates": [[
                [lon, lat],
                [lon + 0.02, lat],
                [lon + 0.02, lat - 0.02],
                [lon + 0.01, lat - 0.03],
                [lon - 0.01, lat - 0.02],
                [lon - 0.01, lat],
                [lon, lat],
            ]],
        })
    return polygons

# ---- Smoothing variants ----
QUANTIZE_BUCKETS = [100, 1000, 2500]
DATA_VARIANTS = ["raw", "q100", "q1000", "q2500"]


def _build_paths():
    """Build nested FORMAT_PATHS: {format_key: {variant: Path}}."""
    paths = {}
    for tag in DATA_VARIANTS:
        suffix = f"_{tag}" if tag != "raw" else ""
        paths[tag] = {
            "cog":             DATA_DIR / f"dem_cog{suffix}.tif",
            "parquet_flat":    DATA_DIR / f"dem_flat{suffix}.parquet",
            "parquet_hilbert":  DATA_DIR / f"dem_hilbert{suffix}.parquet",
            "geoparquet":      DATA_DIR / f"dem{suffix}.geoparquet",
            "zarr":            DATA_DIR / f"dem{suffix}.zarr",
            "lance":           DATA_DIR / f"dem_hilbert{suffix}.lance",
        }

    # Flatten to FORMAT_PATHS[format][variant]
    result = {}
    for fmt_key in ["cog", "parquet_flat", "parquet_hilbert", "geoparquet", "zarr", "lance"]:
        result[fmt_key] = {tag: paths[tag][fmt_key] for tag in DATA_VARIANTS}
    return result


FORMAT_PATHS = _build_paths()


def get_path(format_key: str, variant: str = "raw") -> Path:
    """Return the file path for a format + smoothing variant."""
    return FORMAT_PATHS[format_key][variant]


# Backwards-compatible aliases
COG_PATH = FORMAT_PATHS["cog"]["raw"]
FLAT_PARQUET_PATH = FORMAT_PATHS["parquet_flat"]["raw"]
HILBERT_PARQUET_PATH = FORMAT_PATHS["parquet_hilbert"]["raw"]
GEOPARQUET_PATH = FORMAT_PATHS["geoparquet"]["raw"]
ZARR_PATH = FORMAT_PATHS["zarr"]["raw"]

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
    # Zarr
    ("zarr", "xarray", "point"),
    ("zarr", "xarray", "bbox"),
    ("zarr", "xarray", "polygon"),
    ("zarr", "zarr_native", "polygon"),
    ("zarr", "zarrs_rust", "polygon"),
    # Parquet flat
    ("parquet_flat", "duckdb", "point"),
    ("parquet_flat", "duckdb", "bbox"),
    ("parquet_flat", "duckdb", "polygon"),
    ("parquet_flat", "polars", "point"),
    ("parquet_flat", "polars", "bbox"),
    ("parquet_flat", "polars", "polygon"),
    # Parquet + Zorder
    ("parquet_hilbert", "duckdb", "point"),
    ("parquet_hilbert", "duckdb", "bbox"),
    ("parquet_hilbert", "duckdb", "polygon"),
    ("parquet_hilbert", "polars", "point"),
    ("parquet_hilbert", "polars", "bbox"),
    ("parquet_hilbert", "polars", "polygon"),
    # GeoParquet
    ("geoparquet", "duckdb", "point"),
    ("geoparquet", "duckdb", "bbox"),
    ("geoparquet", "duckdb", "polygon"),
    # Lance
    ("lance", "duckdb", "point"),
    ("lance", "duckdb", "bbox"),
    ("lance", "duckdb", "polygon"),
    ("lance", "lance_scanner", "polygon"),
]

# ---- Display labels ----
FORMAT_LABELS = {
    "cog": "COG",
    "zarr": "Zarr",
    "parquet_flat": "Parquet (flat)",
    "parquet_hilbert": "Parquet + Hilbert",
    "geoparquet": "GeoParquet",
    "lance": "Lance",
}

TOOL_LABELS = {
    "rasterio": "Rasterio",
    "rioxarray": "rioxarray",
    "xarray": "xarray",
    "duckdb": "DuckDB",
    "polars": "Polars",
    "geopandas": "GeoPandas",
    "zarr_native": "Native Zarr",
    "zarrs_rust": "Pure Rust Zarr (zarrs)",
    "lance_scanner": "Lance Scanner",
}

QUERY_LABELS = {
    "point": "Point sample",
    "bbox": "Bbox window",
    "polygon": "Polygon",
}

VARIANT_LABELS = {
    "raw": "Raw",
    "q100": "100m Quantized",
    "q1000": "1000m Quantized",
    "q2500": "2500m Quantized",
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
