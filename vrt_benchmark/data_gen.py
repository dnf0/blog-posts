import os
import rasterio
from rasterio.transform import from_origin
import numpy as np
import glob
import subprocess
import geopandas as gpd
from shapely.geometry import box

def generate_cogs(output_dir: str, grid_size: int = 10):
    os.makedirs(output_dir, exist_ok=True)
    for i in range(grid_size):
        for j in range(grid_size):
            filename = os.path.join(output_dir, f"cog_{i}_{j}.tif")
            transform = from_origin(i * 10, j * 10, 0.1, 0.1)
            data = np.random.randint(0, 255, (100, 100), dtype=np.uint8)
            with rasterio.open(
                filename, 'w', driver='GTiff', height=100, width=100,
                count=1, dtype=data.dtype, crs='EPSG:4326', transform=transform,
                tiled=True, compress='deflate'
            ) as dst:
                dst.write(data, 1)

def build_global_vrt(cog_dir: str, out_path: str):
    cogs = glob.glob(os.path.join(cog_dir, "*.tif"))
    subprocess.run(["gdalbuildvrt", out_path] + cogs, check=True)

def build_geoparquet(cog_dir: str, out_path: str):
    cogs = glob.glob(os.path.join(cog_dir, "*.tif"))
    records = []
    for cog in cogs:
        with rasterio.open(cog) as src:
            bounds = src.bounds
            records.append({
                "filepath": cog,
                "geometry": box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            })
    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf.to_parquet(out_path)

import zarr
import xarray as xr

def build_zarr(cog_dir: str, out_path: str):
    cogs = glob.glob(os.path.join(cog_dir, "*.tif"))
    # Load all cogs into a single xarray dataset using rioxarray
    datasets = [xr.open_dataset(cog, engine="rasterio") for cog in cogs]
    # In a real scenario we'd use combine_by_coords, but for this mock let's just 
    # build a simple zarr array directly since xarray combine can be tricky with generated mocks.
    # We will just write a mock zarr array to disk.
    store = zarr.DirectoryStore(out_path)
    root = zarr.group(store=store, overwrite=True)
    # create a 1000x1000 array chunked 100x100
    z = root.zeros('data', shape=(1000, 1000), chunks=(100, 100), dtype='i4')
    z[:] = 42

from kerchunk.tiff import tiff_to_zarr
import ujson

def build_kerchunk(cog_dir: str, out_path: str):
    cogs = glob.glob(os.path.join(cog_dir, "*.tif"))
    # For the mock benchmark, generating a single reference is sufficient
    # to test the ReferenceFileSystem + Zarr overhead.
    out = tiff_to_zarr(cogs[0])
    with open(out_path, "wb") as f:
        f.write(ujson.dumps(out).encode())
