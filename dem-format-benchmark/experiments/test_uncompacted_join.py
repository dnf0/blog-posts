import duckdb
import pyarrow as pa
import time
import numpy as np
from pathlib import Path
import polars as pl

parquet_path = "data/test_uncompacted.parquet"
if not Path(parquet_path).exists():
    print("Run test_uncompacted.py first")
    exit(1)

# Get some real z_indices
print("Extracting 118 million real z_indices...")
t0 = time.time()
df = pl.read_parquet(parquet_path, columns=["z_index"])
# Sample 118 million (with replacement to simulate many overlapping polygons)
cids = np.random.choice(df["z_index"].to_numpy(), size=118000000, replace=True)
print(f"Extraction took {time.time() - t0:.3f} s")

print("Creating PyArrow table...")
t0 = time.time()
arrow_table = pa.table({'z_index': pa.array(cids)})
print(f"PyArrow creation took {time.time() - t0:.3f} s")

print("Executing DuckDB Hash Join...")
t0 = time.time()
con = duckdb.connect()
con.register('cids_table', arrow_table)
query = f"SELECT AVG(band_value) FROM read_parquet('{parquet_path}') INNER JOIN cids_table USING (z_index)"
res = con.execute(query).fetchone()
con.close()

print(f"DuckDB Query Execution: {time.time() - t0:.3f} s")
print(f"Result: {res}")
