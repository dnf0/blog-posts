import zarr
import rasterio
import rasterio.features
from rasterio.transform import from_bounds
import time
from shapely.geometry import shape
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons

zarr_path = FORMAT_PATHS["zarr"]["q2500"]

# Open Zarr array natively
z = zarr.open(str(zarr_path), mode='r')
arrays = [v for k, v in z.arrays()]
data_array = arrays[0]

# Get transform from COG for the spatial metadata
cog_path = FORMAT_PATHS["cog"]["q2500"]
with rasterio.open(cog_path) as src:
    transform = src.transform

batch_sizes = [10, 100, 1000, 10000]

print("=== Native Zarr Scaling Assessment ===")
for n in batch_sizes:
    polygons = generate_query_polygons(n)
    
    t0 = time.time()
    count = 0
    for poly_geojson in polygons:
        poly = shape(poly_geojson)
        pb = poly.bounds
        
        # Calculate BBox in pixel coordinates
        col_min, row_max = ~transform * (pb[0], pb[1])
        col_max, row_min = ~transform * (pb[2], pb[3])
        
        # Ensure min/max ordering
        if col_min > col_max: col_min, col_max = col_max, col_min
        if row_min > row_max: row_min, row_max = row_max, row_min
        
        # Round and add bounds checking
        c_start = max(0, int(np.floor(col_min)))
        c_stop = min(data_array.shape[-1], int(np.ceil(col_max)))
        r_start = max(0, int(np.floor(row_min)))
        r_stop = min(data_array.shape[-2], int(np.ceil(row_max)))
        
        if r_start >= r_stop or c_start >= c_stop:
            continue
            
        # Read just the bounding box chunk from Zarr
        if len(data_array.shape) == 2:
            cropped_data = data_array[r_start:r_stop, c_start:c_stop]
        else:
            cropped_data = data_array[0, r_start:r_stop, c_start:c_stop]
            
        width = c_stop - c_start
        height = r_stop - r_start
        
        # Create mask
        min_lon = transform.c + c_start * transform.a
        max_lat = transform.f + r_start * transform.e
        window_transform = rasterio.transform.Affine.translation(min_lon, max_lat) * rasterio.transform.Affine.scale(transform.a, transform.e)
        
        mask = rasterio.features.rasterize(
            [(poly, 1)],
            out_shape=(height, width),
            transform=window_transform,
            fill=0,
            dtype=np.uint8
        )
        
        count += np.count_nonzero(mask)

    print(f"Native Zarr ({n} polys): {time.time() - t0:.3f} s, Count: {count}")
