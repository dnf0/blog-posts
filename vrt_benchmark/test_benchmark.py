import pytest
import os
from .data_gen import generate_cogs, build_global_vrt, build_geoparquet
import rasterio
from rasterio.windows import from_bounds
import geopandas as gpd
from shapely.geometry import box
import tempfile
import subprocess

@pytest.fixture(scope="session")
def dataset(tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("data")
    cog_dir = dir_path / "cogs"
    generate_cogs(str(cog_dir), grid_size=10) # 100 COGs
    
    vrt_path = dir_path / "global.vrt"
    build_global_vrt(str(cog_dir), str(vrt_path))
    
    pq_path = dir_path / "index.parquet"
    build_geoparquet(str(cog_dir), str(pq_path))
    
    from .data_gen import build_zarr
    zarr_path = dir_path / "data.zarr"
    build_zarr(str(cog_dir), str(zarr_path))
    
    from .data_gen import build_kerchunk
    kc_path = dir_path / "kerchunk.json"
    build_kerchunk(str(cog_dir), str(kc_path))
    
    from .data_gen import build_lance
    lance_path = dir_path / "data.lance"
    build_lance(str(cog_dir), str(lance_path))

    return {
        "vrt": str(vrt_path),
        "parquet": str(pq_path),
        "cogs": str(cog_dir),
        "zarr": str(zarr_path),
        "kerchunk": str(kc_path),
        "lance": str(lance_path)
    }

def test_bench_global_vrt(benchmark, dataset):
    # bounding box covering ~4 COGs in the middle
    bbox = (45, 45, 55, 55) 
    
    def run_read():
        with rasterio.open(dataset["vrt"]) as src:
            window = from_bounds(*bbox, transform=src.transform)
            return src.read(1, window=window)
            
    result = benchmark(run_read)
    assert result is not None

def test_bench_local_vrt(benchmark, dataset):
    bbox = (45, 45, 55, 55)
    search_geom = box(*bbox)
    
    def run_read():
        # Query index
        gdf = gpd.read_parquet(dataset["parquet"])
        intersecting = gdf[gdf.geometry.intersects(search_geom)]
        files = intersecting["filepath"].tolist()
        
        # Build local VRT dynamically
        with tempfile.NamedTemporaryFile(suffix=".vrt", delete=False) as tmp:
            local_vrt = tmp.name
            
        subprocess.run(["gdalbuildvrt", local_vrt] + files, check=True, capture_output=True)
        
        # Read from local VRT
        with rasterio.open(local_vrt) as src:
            window = from_bounds(*bbox, transform=src.transform)
            data = src.read(1, window=window)
            
        os.remove(local_vrt)
        return data

    result = benchmark(run_read)
    assert result is not None

from rasterio.merge import merge

def test_bench_geoparquet_direct(benchmark, dataset):
    bbox = (45, 45, 55, 55)
    search_geom = box(*bbox)
    
    def run_read():
        gdf = gpd.read_parquet(dataset["parquet"])
        intersecting = gdf[gdf.geometry.intersects(search_geom)]
        files = intersecting["filepath"].tolist()
        
        srcs = [rasterio.open(f) for f in files]
        try:
            mosaic, out_trans = merge(srcs, bounds=bbox)
            return mosaic
        finally:
            for src in srcs:
                src.close()
                
    result = benchmark(run_read)
    assert result is not None

import zarr

def test_bench_zarr(benchmark, dataset):
    # bbox in pixels (mocking the spatial query)
    # 45,45 to 55,55 in spatial translates to pixel slices in our mock 1000x1000 Zarr.
    # Let's say pixels 450:550
    def run_read():
        store = zarr.DirectoryStore(dataset["zarr"])
        root = zarr.group(store=store)
        data = root['data'][450:550, 450:550]
        return data

    result = benchmark(run_read)
    assert result is not None

import fsspec
import xarray as xr

def test_bench_kerchunk(benchmark, dataset):
    def run_read():
        fs = fsspec.filesystem(
            "reference", fo=dataset["kerchunk"], remote_protocol="file"
        )
        m = fs.get_mapper("")
        z = zarr.open(m, mode="r")
        # Mock spatial read by pulling a slice
        return z[45:55, 45:55]

    result = benchmark(run_read)
    assert result is not None

import threading
import uvicorn
import time
import httpx
from fastapi import FastAPI
from fastapi.responses import Response

# Mock FastAPI app
app = FastAPI()

@app.get("/mosaic/bbox")
def read_mosaic(bbox: str):
    # Simulate a TiTiler dynamic read
    # In real life, it would query STAC, open COGs, and mosaic.
    # We will simulate ~50ms of compute + network serialization
    time.sleep(0.05)
    return Response(content=b"mock_tiff_data", media_type="image/tiff")

class ServerThread(threading.Thread):
    def __init__(self, app, host="127.0.0.1", port=8000):
        super().__init__()
        self.server = uvicorn.Server(config=uvicorn.Config(app, host=host, port=port, log_level="critical"))
    def run(self):
        self.server.run()
    def stop(self):
        self.server.should_exit = True
        self.join()

@pytest.fixture(scope="session")
def titiler_server():
    server = ServerThread(app)
    server.start()
    time.sleep(1) # wait for startup
    yield "http://127.0.0.1:8000"
    server.stop()

def test_bench_titiler(benchmark, titiler_server):
    client = httpx.Client()
    def run_read():
        resp = client.get(f"{titiler_server}/mosaic/bbox?bbox=45,45,55,55")
        resp.raise_for_status()
        return resp.content
        
    result = benchmark(run_read)
    assert result is not None
    client.close()

from lance.dataset import LanceDataset
from shapely import wkb

def test_bench_lance(benchmark, dataset):
    bbox = box(45, 45, 55, 55)
    
    def run_read():
        ds = LanceDataset(dataset["lance"])
        # Fetch the entire dataset as a pyarrow table
        table = ds.to_table()
        
        # We simulate checking geometry intersection on the client side since lance's
        # python API doesn't have a direct spatial filter (though LanceDB does)
        # We can extract the WKB column, check intersection, then take the images
        wkbs = table["geometry_wkb"].to_pylist()
        indices = []
        for i, geom_bytes in enumerate(wkbs):
            geom = wkb.loads(geom_bytes)
            if geom.intersects(bbox):
                indices.append(i)
                
        # Take just the intersecting rows directly using random access via "take"
        if indices:
            result_table = ds.take(indices)
            return result_table
        return None

    result = benchmark(run_read)
    assert result is not None
