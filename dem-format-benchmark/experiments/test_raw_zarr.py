import time
import sys
from pathlib import Path
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons
import zarrs_plugin
import rasterio

cog_path = str(FORMAT_PATHS["cog"]["raw"])
zarr_path = str(FORMAT_PATHS["zarr"]["raw"])

polygons = generate_query_polygons(1000, seed=42)
exterior_rings_tuples = []
for polygon_geojson in polygons:
    exterior_ring = polygon_geojson["coordinates"][0]
    exterior_rings_tuples.append([tuple(coord) for coord in exterior_ring])

with rasterio.open(cog_path) as src:
    transform = src.transform
    t_tuple = (transform.a, transform.b, transform.c, transform.d, transform.e, transform.f)

t0 = time.time()
count = zarrs_plugin.zarrs_polygon_query(zarr_path, exterior_rings_tuples, t_tuple)
print(f"Raw Zarr (1000 polys): {time.time() - t0:.3f} s (Count: {count})")
