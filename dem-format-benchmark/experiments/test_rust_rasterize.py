import time
import rasterio
import rasterio.features
from rasterio.transform import from_bounds
from shapely.geometry import shape
import numpy as np
import sys
from pathlib import Path

sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import generate_query_polygons

polygons = generate_query_polygons(1, seed=42)
poly = shape(polygons[0])
pb = poly.bounds

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
mask = rasterio.features.rasterize([(poly, 1)], out_shape=(height, width), transform=transform, fill=0, dtype=np.uint8)
rows, cols = np.nonzero(mask)

print("GDAL Rasterize Count:", len(cols))
