import duckdb
import pyarrow as pa
import time
import numpy as np
from pathlib import Path
from shapely.geometry import shape

import sys
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
from scripts.hilbert_plugin import hilbert_cells_for_polygons

parquet_path = "data/test_uncompacted.parquet"

polygons = generate_query_polygons(1, seed=42)
exterior_ring = polygons[0]["coordinates"][0]
rings = [[tuple(coord) for coord in exterior_ring]]
import polars_hilbert
cids = polars_hilbert.hilbert_cells_for_polygons(rings)
cids = [(c >> 4) * 16 for c in cids]

con = duckdb.connect()
arrow_table = pa.table({'z_index': pa.array(cids, type=pa.uint64())})
con.register('cids_table', arrow_table)

query = f"EXPLAIN ANALYZE SELECT AVG(band_value) FROM read_parquet('{parquet_path}') INNER JOIN cids_table USING (z_index)"
res = con.execute(query).fetchall()

for row in res:
    print(row[0])
    print(row[1])
