import pyarrow.parquet as pq
import pyarrow as pa
import duckdb
import time
import numpy as np
from pathlib import Path
from shapely.geometry import shape

import sys
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
from scripts.hilbert_plugin import hilbert_cells_for_polygons
import polars as pl

# Load the uncompacted data into Arrow so we can write it with custom settings
print("Loading baseline data...")
df = pl.read_parquet("data/test_uncompacted.parquet")
arrow_table = df.to_arrow()

# Define test files
small_rg_path = "data/test_small_rg.parquet"
bloom_path = "data/test_bloom.parquet"

# Write with small row groups (e.g., 10,000 rows instead of 100,000+)
if not Path(small_rg_path).exists():
    print("Writing Small Row Group Parquet...")
    pq.write_table(arrow_table, small_rg_path, compression="zstd", row_group_size=10000)
print(f"Small RG File Size: {Path(small_rg_path).stat().st_size / 1024 / 1024:.2f} MB")

# The correct way to write bloom filters in pyarrow:
# Let's just use kwargs if supported, else use page index
if not Path("data/test_page_index.parquet").exists():
    print("Writing Page Index Parquet...")
    pq.write_table(arrow_table, "data/test_page_index.parquet", compression="zstd", write_page_index=True, write_statistics=True)
print(f"Page Index File Size: {Path('data/test_page_index.parquet').stat().st_size / 1024 / 1024:.2f} MB")


# Get polygon cids
polygons = generate_query_polygons(1, seed=42)
exterior_ring = polygons[0]["coordinates"][0]
rings = [[tuple(coord) for coord in exterior_ring]]
import polars_hilbert
cids = polars_hilbert.hilbert_cells_for_polygons(rings)
cids = [(c >> 4) * 16 for c in cids]

con = duckdb.connect()
arrow_cids = pa.table({'z_index': pa.array(cids, type=pa.uint64())})
con.register('cids_table', arrow_cids)

# Test function
def test_query(name, path):
    print(f"\n--- {name} ---")
    query = f"EXPLAIN ANALYZE SELECT AVG(band_value) FROM read_parquet('{path}') INNER JOIN cids_table USING (z_index)"
    res = con.execute(query).fetchall()
    
    table_scan_lines = []
    in_table_scan = False
    for row in res:
        lines = row[1].split('\n')
        for line in lines:
            if "TABLE_SCAN" in line:
                in_table_scan = True
            if in_table_scan and "Total Files Read" in line:
                pass
            if in_table_scan and "rows" in line and "s" in line: # rudimentary parsing
                pass
        # Just print the explain analyze
        print(row[1])

print("\n--- BASELINE (Uncompacted) ---")
for r in con.execute(f"EXPLAIN ANALYZE SELECT AVG(band_value) FROM read_parquet('data/test_uncompacted.parquet') INNER JOIN cids_table USING (z_index)").fetchall():
    print(r[1])

print("\n--- SMALL ROW GROUPS ---")
for r in con.execute(f"EXPLAIN ANALYZE SELECT AVG(band_value) FROM read_parquet('{small_rg_path}') INNER JOIN cids_table USING (z_index)").fetchall():
    print(r[1])

print("\n--- PAGE INDEX ---")
for r in con.execute(f"EXPLAIN ANALYZE SELECT AVG(band_value) FROM read_parquet('data/test_page_index.parquet') INNER JOIN cids_table USING (z_index)").fetchall():
    print(r[1])
