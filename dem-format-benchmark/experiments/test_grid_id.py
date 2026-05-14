import polars as pl
import rasterio
import duckdb
import pyarrow as pa
import time
import numpy as np
import sys
from pathlib import Path
from shapely.geometry import shape

sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
from scripts.hilbert_plugin import compute_hilbert

parquet_path = "data/test_uncompacted_grid.parquet"
cog_path = str(FORMAT_PATHS["cog"]["raw"])

if not Path(parquet_path).exists():
    print(f"Generating {parquet_path}...")
    with rasterio.open(cog_path) as src:
        transform = src.transform

    q = (
        pl.scan_parquet("data/dem_base_raw.parquet")
        .with_columns([
            ((pl.col("band_value") / 2500).round() * 2500).cast(pl.Int16).alias("band_value"),
            (((transform.c + pl.col("col") * transform.a) + 180.0) * 3600.0).round().cast(pl.UInt32).alias("global_col"),
            ((90.0 - (transform.f + pl.col("row") * transform.e)) * 3600.0).round().cast(pl.UInt32).alias("global_row")
        ])
        .with_columns([
            (compute_hilbert("global_col", "global_row") * 16).alias("z_index"),
            # Create a 10km grid (approx 300 pixels at 30m)
            (pl.col("global_col") // 300).cast(pl.UInt16).alias("grid_x"),
            (pl.col("global_row") // 300).cast(pl.UInt16).alias("grid_y"),
        ])
        .sort(["grid_x", "grid_y", "z_index"])
        .select(["grid_x", "grid_y", "band_value", "z_index"])
    )
    q.sink_parquet(parquet_path, compression="zstd")
    print(f"Generated file. Size: {Path(parquet_path).stat().st_size / 1024 / 1024:.2f} MB")

polygons = generate_query_polygons(1, seed=42)
poly = shape(polygons[0])
pb = poly.bounds

with rasterio.open(cog_path) as src:
    transform = src.transform

col_min, row_max = ~transform * (pb[0], pb[1])
col_max, row_min = ~transform * (pb[2], pb[3])
c_start = max(0, int(np.floor(col_min)))
c_stop = min(21600, int(np.ceil(col_max)))
r_start = max(0, int(np.floor(row_min)))
r_stop = min(21600, int(np.ceil(row_max)))

global_c_start = c_start + 8964
global_c_stop = c_stop + 8964
global_r_start = r_start + 12600
global_r_stop = r_stop + 12600

grid_x_min = global_c_start // 300
grid_x_max = global_c_stop // 300
grid_y_min = global_r_start // 300
grid_y_max = global_r_stop // 300

exterior_ring = polygons[0]["coordinates"][0]
rings = [[tuple(coord) for coord in exterior_ring]]
import polars_hilbert
cids = polars_hilbert.hilbert_cells_for_polygons(rings)
cids = [(c >> 4) * 16 for c in cids]

con = duckdb.connect()
arrow_table = pa.table({'z_index': pa.array(cids, type=pa.uint64())})
con.register('cids_table', arrow_table)

print("\n--- Query WITH Grid Bounds ---")
query_grid = f"""
EXPLAIN ANALYZE 
SELECT AVG(band_value) 
FROM read_parquet('{parquet_path}') 
INNER JOIN cids_table USING (z_index)
WHERE grid_x >= {grid_x_min} AND grid_x <= {grid_x_max}
  AND grid_y >= {grid_y_min} AND grid_y <= {grid_y_max}
"""
for row in con.execute(query_grid).fetchall():
    print(row[0])
    print(row[1])
