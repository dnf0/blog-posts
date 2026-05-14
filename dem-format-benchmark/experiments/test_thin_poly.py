import rasterio.features
from rasterio.transform import from_bounds
from shapely.geometry import LineString
import numpy as np

# Create a "thin" diagonal road/pipeline ~5km long, buffered by ~10 meters (0.0001 degrees)
road = LineString([(8.5, 46.5), (8.55, 46.55)]).buffer(0.0001)

pb = road.bounds
col_min = int(round((pb[0] + 180.0) * 3600.0))
col_max = int(round((pb[2] + 180.0) * 3600.0))
row_max = int(round((90.0 - pb[1]) * 3600.0))
row_min = int(round((90.0 - pb[3]) * 3600.0))

if col_min > col_max: col_min, col_max = col_max, col_min
if row_min > row_max: row_min, row_max = row_max, row_min

width = col_max - col_min + 1
height = row_max - row_min + 1

min_lon = col_min / 3600.0 - 180.0
max_lon = (col_max + 1) / 3600.0 - 180.0
max_lat = 90.0 - row_min / 3600.0
min_lat = 90.0 - (row_max + 1) / 3600.0

transform = from_bounds(min_lon, min_lat, max_lon, max_lat, width, height)

# 1. GDAL Center-Point (Strict)
mask_strict = rasterio.features.rasterize([(road, 1)], out_shape=(height, width), transform=transform, fill=0, dtype=np.uint8, all_touched=False)
count_strict = np.count_nonzero(mask_strict)

# 2. GDAL All-Touched (Loose / Boundary inclusive)
mask_touched = rasterio.features.rasterize([(road, 1)], out_shape=(height, width), transform=transform, fill=0, dtype=np.uint8, all_touched=True)
count_touched = np.count_nonzero(mask_touched)

print(f"Thin Polygon (Road/Pipeline):")
print(f"Strict Center-Point Count: {count_strict} pixels")
print(f"All-Touched Count:         {count_touched} pixels")
print(f"Discrepancy:               {abs(count_touched - count_strict)} pixels ({(count_touched / count_strict - 1) * 100:.1f}%)")
