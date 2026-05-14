import time
import os
import rasterio
import rasterio.mask
import numpy as np
import zarrs_plugin
import polars as pl
import duckdb
from shapely.geometry import shape
import sys
from pathlib import Path
import pyarrow as pa

sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
from scripts.hilbert_plugin import hilbert_cells_for_polygons

def clear_cache():
    os.system("sudo purge > /dev/null 2>&1")

cog_path = str(FORMAT_PATHS["cog"]["raw"])
zarr_path = str(FORMAT_PATHS["zarr"]["q2500"])
hilbert_path = str(FORMAT_PATHS["parquet_hilbert"]["q2500"])

# 50 polygons is a reasonable sample size to get an average cold-start latency per polygon
# without waiting hours for the `sudo purge` commands to finish.
polygons = generate_query_polygons(50, seed=42)

with rasterio.open(cog_path) as src:
    transform = src.transform
    t_tuple = (transform.a, transform.b, transform.c, transform.d, transform.e, transform.f)

print("=== True Cold-Start Assessment (50 Polygons, Purge Per Polygon) ===")
print("Note: If prompted, please enter your sudo password to allow cache clearing.\n")

# 1. Rasterio
print("Testing Rasterio + COG...")
t_rasterio = 0
count_r = 0
for poly_geojson in polygons:
    clear_cache()
    
    t_start = time.time()
    poly = shape(poly_geojson)
    with rasterio.open(cog_path) as src:
        masked_data, _ = rasterio.mask.mask(src, [poly], crop=True)
        valid_mask = np.isfinite(masked_data)
        if src.nodata is not None and np.isfinite(src.nodata):
            valid_mask &= (masked_data != src.nodata)
        count_r += np.count_nonzero(valid_mask)
    t_rasterio += (time.time() - t_start)

# 2. Pure Rust Zarr
print("Testing Pure Rust Zarr...")
t_zarr = 0
count_z = 0
for poly_geojson in polygons:
    clear_cache()
    
    t_start = time.time()
    exterior_ring = poly_geojson["coordinates"][0]
    rings = [[tuple(coord) for coord in exterior_ring]]
    count_z += zarrs_plugin.zarrs_polygon_query(zarr_path, rings, t_tuple)
    t_zarr += (time.time() - t_start)

# 3. DuckDB + Parquet
print("Testing DuckDB + Parquet...")
t_duckdb = 0
count_p = 0
for poly_geojson in polygons:
    clear_cache()
    
    t_start = time.time()
    exterior_ring = poly_geojson["coordinates"][0]
    rings = [[tuple(coord) for coord in exterior_ring]]
    unique_cids = hilbert_cells_for_polygons(rings)
    if unique_cids:
        con = duckdb.connect()
        arrow_table = pa.table({'z_index': pa.array(unique_cids, type=pa.uint64())})
        con.register('cids_table', arrow_table)
        query = f"SELECT COUNT(*) FROM read_parquet('{hilbert_path}') INNER JOIN cids_table USING (z_index)"
        res = con.execute(query).fetchone()
        count_p += res[0]
        con.close()
    t_duckdb += (time.time() - t_start)

print(f"\n--- Results for 50 Cold-Start Polygons ---")
print(f"Rasterio + COG:    {t_rasterio:.3f} s (Avg per poly: {t_rasterio/50:.4f} s)")
print(f"Pure Rust Zarr:    {t_zarr:.3f} s (Avg per poly: {t_zarr/50:.4f} s)")
print(f"DuckDB + Parquet:  {t_duckdb:.3f} s (Avg per poly: {t_duckdb/50:.4f} s)")

print(f"\n--- Extrapolated 100,000 Global Cold-Start Polygons ---")
print(f"Rasterio + COG:    {(t_rasterio/50) * 100000:,.0f} s ({(t_rasterio/50) * 100000 / 3600:.1f} hours)")
print(f"Pure Rust Zarr:    {(t_zarr/50) * 100000:,.0f} s ({(t_zarr/50) * 100000 / 3600:.1f} hours)")
print(f"DuckDB + Parquet:  {(t_duckdb/50) * 100000:,.0f} s ({(t_duckdb/50) * 100000 / 3600:.1f} hours)")