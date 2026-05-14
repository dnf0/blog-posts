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

parquet_path = "data/test_uncompacted_xy.parquet"
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
            (compute_hilbert("global_col", "global_row") * 16).alias("z_index")
        ])
        .sort("z_index")
        .select(["global_col", "global_row", "band_value", "z_index"])
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

global_c_start = int(round((pb[0] + 180.0) * 3600.0))
global_c_stop = int(round((pb[2] + 180.0) * 3600.0))
global_r_stop = int(round((90.0 - pb[1]) * 3600.0))
global_r_start = int(round((90.0 - pb[3]) * 3600.0))

exterior_ring = polygons[0]["coordinates"][0]
rings = [[tuple(coord) for coord in exterior_ring]]
import polars_hilbert
cids = polars_hilbert.hilbert_cells_for_polygons(rings)
cids = [(c >> 4) * 16 for c in cids]

con = duckdb.connect()
arrow_table = pa.table({'z_index': pa.array(cids, type=pa.uint64())})
con.register('cids_table', arrow_table)

print("\n--- Query WITHOUT X/Y Bounds ---")
query_no_xy = f"""
EXPLAIN ANALYZE 
SELECT AVG(band_value) 
FROM read_parquet('{parquet_path}') 
INNER JOIN cids_table USING (z_index)
"""
for row in con.execute(query_no_xy).fetchall():
    print(row[0])
    print(row[1])

print("\n--- Query WITH X/Y Bounds ---")
query_xy = f"""
EXPLAIN ANALYZE 
SELECT AVG(band_value) 
FROM read_parquet('{parquet_path}') 
INNER JOIN cids_table USING (z_index)
WHERE global_col >= {global_c_start} AND global_col <= {global_c_stop}
  AND global_row >= {global_r_start} AND global_row <= {global_r_stop}
"""
for row in con.execute(query_xy).fetchall():
    print(row[0])
    print(row[1])
