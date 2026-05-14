import lance
import polars as pl
import duckdb
import pyarrow as pa
import time
import numpy as np
import sys
from pathlib import Path
from shapely.geometry import shape

sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
from scripts.hilbert_plugin import hilbert_cells_for_polygons

parquet_xy_path = "data/test_uncompacted_xy.parquet"
lance_xy_path = "data/test_uncompacted_xy.lance"

if not Path(parquet_xy_path).exists():
    print(f"Run test_xy_zonemap.py first to generate {parquet_xy_path}")
    sys.exit(1)

# 1. Convert Parquet (with X/Y) to Lance
if not Path(lance_xy_path).exists():
    print(f"Converting {parquet_xy_path} to Lance format...")
    t0 = time.time()
    df = pl.read_parquet(parquet_xy_path)
    
    # Lance requires an index to be fast. Creating a dataset doesn't automatically index everything perfectly for 2D.
    # We will just write it and see the base size and scan performance.
    dataset = lance.write_dataset(df.to_arrow(), lance_xy_path)
    print(f"Conversion complete in {time.time() - t0:.2f}s.")

import subprocess
size = subprocess.check_output(f"du -sh {lance_xy_path}", shell=True).decode().split()[0]
print(f"Lance XY Dataset Size: {size}")

# Load Lance dataset
dataset = lance.dataset(lance_xy_path)

# Prepare Query bounds
polygons = generate_query_polygons(1, seed=42)
poly = shape(polygons[0])
pb = poly.bounds

import rasterio
cog_path = str(FORMAT_PATHS["cog"]["raw"])
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

print("\n--- Query Lance WITHOUT X/Y Bounds ---")
query_no_xy = f"""
EXPLAIN ANALYZE 
SELECT AVG(band_value) 
FROM lance_scan('{lance_xy_path}') 
INNER JOIN cids_table USING (z_index)
"""
try:
    for row in con.execute(query_no_xy).fetchall():
        print(row[0])
        print(row[1])
except Exception as e:
    print("Native lance_scan failed, falling back to pyarrow scanner")
    lance_table = dataset.to_table()
    con.register('lance_table', lance_table)
    query_no_xy = "EXPLAIN ANALYZE SELECT AVG(band_value) FROM lance_table INNER JOIN cids_table USING (z_index)"
    for row in con.execute(query_no_xy).fetchall():
        print(row[0])
        print(row[1])

print("\n--- Query Lance WITH X/Y Bounds ---")
t0 = time.time()
# Push down X/Y bounds to Lance directly via string filter
filter_str = f"global_col >= {global_c_start} AND global_col <= {global_c_stop} AND global_row >= {global_r_start} AND global_row <= {global_r_stop}"
filtered_table = dataset.scanner(filter=filter_str).to_table()

if len(filtered_table) > 0:
    con.register('filtered_table', filtered_table)
    query = "SELECT AVG(band_value) FROM filtered_table INNER JOIN cids_table USING (z_index)"
    res = con.execute(query).fetchone()
else:
    res = (None,)
print(f"Lance X/Y Scanner + DuckDB Join Time: {time.time() - t0:.3f} s")
print(f"Result: {res}")
