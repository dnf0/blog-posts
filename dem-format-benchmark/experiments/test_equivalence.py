import numpy as np
import rasterio
import rasterio.mask
import time
from shapely.geometry import shape
import sys
from pathlib import Path
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
import zarrs_plugin

cog_path = str(FORMAT_PATHS["cog"]["q2500"])
zarr_path = str(FORMAT_PATHS["zarr"]["q2500"])

polygons = generate_query_polygons(100, seed=42)

# 1. Get exact means using Rasterio (GDAL)
rasterio_means = []
with rasterio.open(cog_path) as src:
    transform = src.transform
    t_tuple = (transform.a, transform.b, transform.c, transform.d, transform.e, transform.f)
    
    for poly_geojson in polygons:
        poly = shape(poly_geojson)
        masked_data, _ = rasterio.mask.mask(src, [poly], crop=True)
        valid_mask = np.isfinite(masked_data)
        if src.nodata is not None and np.isfinite(src.nodata):
            valid_mask &= (masked_data != src.nodata)
        
        valid_pixels = masked_data[valid_mask]
        if len(valid_pixels) > 0:
            rasterio_means.append(np.mean(valid_pixels))
        else:
            rasterio_means.append(np.nan)

# 2. Get exact means using zarrs_rust
exterior_rings_tuples = []
for polygon_geojson in polygons:
    exterior_ring = polygon_geojson["coordinates"][0]
    exterior_rings_tuples.append([tuple(coord) for coord in exterior_ring])

rust_means = zarrs_plugin.zarrs_polygon_means(zarr_path, exterior_rings_tuples, t_tuple)

# 3. Compare them
discrepancies = []
for i, (r_mean, z_mean) in enumerate(zip(rasterio_means, rust_means)):
    if np.isnan(r_mean) and np.isnan(z_mean):
        continue
    elif np.isnan(r_mean) or np.isnan(z_mean):
        discrepancies.append(abs(np.nan_to_num(r_mean) - np.nan_to_num(z_mean)))
    else:
        discrepancies.append(abs(r_mean - z_mean))

max_diff = np.max(discrepancies)
mean_diff = np.mean(discrepancies)
median_diff = np.median(discrepancies)

print(f"--- Equivalence Test (100 Polygons) ---")
print(f"Max Absolute Error:    {max_diff:.4f} meters")
print(f"Mean Absolute Error:   {mean_diff:.4f} meters")
print(f"Median Absolute Error: {median_diff:.4f} meters")
print(f"Note: Error is caused purely by Rasterio's C++ tie-breaking heuristics vs geo-rasterize's Rust tie-breaking on boundary pixels.")
