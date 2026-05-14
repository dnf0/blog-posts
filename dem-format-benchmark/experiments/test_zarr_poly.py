import xarray as xr
import rioxarray
import time
from shapely.geometry import shape
import sys
from pathlib import Path
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons

zarr_path = FORMAT_PATHS["zarr"]["q2500"]
polygons = generate_query_polygons(100)

t0 = time.time()
ds = xr.open_zarr(zarr_path)
ds.rio.write_crs("EPSG:4326", inplace=True)
count = 0
for poly_geojson in polygons:
    poly = shape(poly_geojson)
    clipped = ds.rio.clip([poly.__geo_interface__], ds.rio.crs).compute()
    # It's a Dataset, let's get the first data variable
    var_name = list(clipped.data_vars)[0]
    count += clipped[var_name].size
ds.close()
print(f"Xarray Zarr (100 polys): {time.time() - t0:.3f} s, Count: {count}")
