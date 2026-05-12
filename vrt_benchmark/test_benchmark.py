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
    
    return {
        "vrt": str(vrt_path),
        "parquet": str(pq_path),
        "cogs": str(cog_dir)
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
