import lance
import polars as pl
import time
import os
from pathlib import Path
import sys

sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
from scripts.hilbert_plugin import hilbert_cells_for_polygons
import subprocess

parquet_path = str(FORMAT_PATHS["parquet_hilbert"]["q2500"])
lance_path = "data/dem_hilbert_q2500.lance"

# 1. Convert Parquet to Lance
if not Path(lance_path).exists():
    print(f"Converting {parquet_path} to Lance format...")
    df = pl.read_parquet(parquet_path)
    # Write to Lance
    lance.write_dataset(df.to_arrow(), lance_path)
    print("Conversion complete.")

# Check size
size = subprocess.check_output(f"du -sh {lance_path}", shell=True).decode().split()[0]
print(f"Lance Dataset Size: {size}")

# Load Lance dataset
dataset = lance.dataset(lance_path)

# 2. Benchmark Query
batch_sizes = [10, 100, 1000]

print("\n=== Lance Scaling Assessment ===")
for n in batch_sizes:
    polygons = generate_query_polygons(n=n, seed=42)
    
    t0 = time.time()
    # Cover
    t_cover = time.time()
    exterior_rings_tuples = []
    for polygon_geojson in polygons:
        exterior_ring = polygon_geojson["coordinates"][0]
        exterior_rings_tuples.append([tuple(coord) for coord in exterior_ring])
        
    import polars_hilbert
    unique_cids = polars_hilbert.hilbert_cells_for_polygons(exterior_rings_tuples)
    cover_time = time.time() - t_cover
    
    # Query using Lance scanner
    t_query = time.time()
    if unique_cids:
        # Lance supports DuckDB out of the box, but let's try Lance's native scanner first via Arrow
        # We can pass an IN clause to Lance using PyArrow compute expressions, or DuckDB
        import duckdb
        con = duckdb.connect()
        import pyarrow as pa
        arrow_table = pa.table({'z_index': pa.array(unique_cids, type=pa.uint64())})
        con.register('cids_table', arrow_table)
        # DuckDB can query Lance datasets natively if lancedb/lance is installed!
        query = f"SELECT COUNT(*) FROM lance_scan('{lance_path}') INNER JOIN cids_table USING (z_index)"
        try:
            res = con.execute(query).fetchone()
            count = res[0]
        except Exception:
            # Fallback if DuckDB doesn't natively have lance_scan without extension
            # Let's use the standard pyarrow scanner via duckdb integration
            lance_table = dataset.to_table()
            con.register('lance_table', lance_table)
            query = f"SELECT COUNT(*) FROM lance_table INNER JOIN cids_table USING (z_index)"
            res = con.execute(query).fetchone()
            count = res[0]
        con.close()
    else:
        count = 0
    query_time = time.time() - t_query
    
    total_time = time.time() - t0
    print(f"Lance + DuckDB ({n} polys): {total_time:7.3f} s (Cover: {cover_time:5.3f}s, Query: {query_time:5.3f}s) (Count approx: {count})")
