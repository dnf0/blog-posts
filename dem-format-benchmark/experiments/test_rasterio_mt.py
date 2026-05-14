import time
import rasterio
import rasterio.mask
import numpy as np
from shapely.geometry import shape
import concurrent.futures
import sys
from pathlib import Path
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, generate_query_polygons

cog_path = FORMAT_PATHS["cog"]["q2500"]
polygons = generate_query_polygons(1000, seed=42)

def benchmark_rasterio(cog_path, polygons):
    t0 = time.time()
    count = 0
    with rasterio.open(cog_path) as src:
        for poly_geojson in polygons:
            poly = shape(poly_geojson)
            masked_data, _ = rasterio.mask.mask(src, [poly], crop=True)
            valid_mask = np.isfinite(masked_data)
            if src.nodata is not None and np.isfinite(src.nodata):
                valid_mask &= (masked_data != src.nodata)
            count += np.count_nonzero(valid_mask)
    return time.time() - t0, count

def process_poly(poly_geojson):
    with rasterio.open(cog_path) as src:
        poly = shape(poly_geojson)
        masked_data, _ = rasterio.mask.mask(src, [poly], crop=True)
        valid_mask = np.isfinite(masked_data)
        if src.nodata is not None and np.isfinite(src.nodata):
            valid_mask &= (masked_data != src.nodata)
        return np.count_nonzero(valid_mask)

def benchmark_rasterio_mt(cog_path, polygons):
    t0 = time.time()
    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(process_poly, polygons))
    return time.time() - t0, sum(results)

def benchmark_rasterio_mp(cog_path, polygons):
    t0 = time.time()
    with concurrent.futures.ProcessPoolExecutor() as executor:
        results = list(executor.map(process_poly, polygons))
    return time.time() - t0, sum(results)

print("Sequential:", benchmark_rasterio(cog_path, polygons))
print("ThreadPool:", benchmark_rasterio_mt(cog_path, polygons))
print("ProcessPool:", benchmark_rasterio_mp(cog_path, polygons))
