import numpy as np
import pyarrow.parquet as pq
import zarr
import sys
from pathlib import Path
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
from scripts.hilbert_plugin import hilbert_cells_for_polygons

# Uncompacted parquet
parquet_path = "data/test_uncompacted.parquet"
zarr_path = str(FORMAT_PATHS["zarr"]["q2500"])

polygons = generate_query_polygons(1, seed=42)

# Zarr chunks touched
z = zarr.open(zarr_path, mode='r')
arrays = [v for k, v in z.arrays()]
data_array = arrays[0]

import rasterio
from shapely.geometry import shape

cog_path = str(FORMAT_PATHS["cog"]["raw"])
with rasterio.open(cog_path) as src:
    transform = src.transform

poly = shape(polygons[0])
pb = poly.bounds

col_min, row_max = ~transform * (pb[0], pb[1])
col_max, row_min = ~transform * (pb[2], pb[3])
c_start = max(0, int(np.floor(col_min)))
c_stop = min(data_array.shape[-1], int(np.ceil(col_max)))
r_start = max(0, int(np.floor(row_min)))
r_stop = min(data_array.shape[-2], int(np.ceil(row_max)))

# Zarr chunk size
chunk_h, chunk_w = data_array.chunks[-2:]
chunk_r_start = r_start // chunk_h
chunk_r_stop = (r_stop - 1) // chunk_h
chunk_c_start = c_start // chunk_w
chunk_c_stop = (c_stop - 1) // chunk_w

zarr_chunks_touched = (chunk_r_stop - chunk_r_start + 1) * (chunk_c_stop - chunk_c_start + 1)
zarr_chunk_bytes = chunk_h * chunk_w * 2 # uncompressed i16
zarr_overfetch_bytes = zarr_chunks_touched * zarr_chunk_bytes

# Parquet row groups touched
exterior_ring = polygons[0]["coordinates"][0]
rings = [[tuple(coord) for coord in exterior_ring]]
import polars_hilbert
cids = polars_hilbert.hilbert_cells_for_polygons(rings)
cids = [(c >> 4) * 16 for c in cids] # Convert to base level 0

min_cid = min(cids)
max_cid = max(cids)

pq_file = pq.ParquetFile(parquet_path)
rg_touched = 0
rg_total_bytes = 0

for i in range(pq_file.num_row_groups):
    rg = pq_file.metadata.row_group(i)
    col_meta = rg.column(1)
    if col_meta.statistics:
        rg_min = col_meta.statistics.min
        rg_max = col_meta.statistics.max
        # Check overlap
        if rg_max >= min_cid and rg_min <= max_cid:
            rg_touched += 1
            rg_total_bytes += rg.total_byte_size # Uncompressed

print(f"Zarr chunks touched: {zarr_chunks_touched} ({zarr_overfetch_bytes / 1024 / 1024:.2f} MB uncompressed)")
print(f"Parquet row groups touched: {rg_touched} out of {pq_file.num_row_groups} ({rg_total_bytes / 1024 / 1024:.2f} MB uncompressed)")
