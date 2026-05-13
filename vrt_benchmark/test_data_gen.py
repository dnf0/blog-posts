import os
from .data_gen import generate_cogs, build_global_vrt, build_geoparquet
import geopandas as gpd

def test_generate_cogs(tmp_path):
    output_dir = tmp_path / "cogs"
    output_dir.mkdir()
    generate_cogs(str(output_dir), grid_size=2)
    assert len(list(output_dir.glob("*.tif"))) == 4

def test_build_indices(tmp_path):
    cog_dir = tmp_path / "cogs"
    cog_dir.mkdir()
    generate_cogs(str(cog_dir), grid_size=2)
    
    vrt_path = tmp_path / "global.vrt"
    build_global_vrt(str(cog_dir), str(vrt_path))
    assert vrt_path.exists()
    
    pq_path = tmp_path / "index.parquet"
    build_geoparquet(str(cog_dir), str(pq_path))
    df = gpd.read_parquet(pq_path)
    assert len(df) == 4

from .data_gen import build_zarr

def test_build_zarr(tmp_path):
    cog_dir = tmp_path / "cogs"
    cog_dir.mkdir()
    generate_cogs(str(cog_dir), grid_size=2)
    
    zarr_path = tmp_path / "data.zarr"
    build_zarr(str(cog_dir), str(zarr_path))
    assert zarr_path.exists()

from .data_gen import build_kerchunk

def test_build_kerchunk(tmp_path):
    cog_dir = tmp_path / "cogs"
    cog_dir.mkdir()
    generate_cogs(str(cog_dir), grid_size=2)
    
    kc_path = tmp_path / "index.json"
    build_kerchunk(str(cog_dir), str(kc_path))
    assert kc_path.exists()

from .data_gen import build_lance

def test_build_lance(tmp_path):
    cog_dir = tmp_path / "cogs"
    cog_dir.mkdir()
    generate_cogs(str(cog_dir), grid_size=2)
    
    lance_path = tmp_path / "data.lance"
    build_lance(str(cog_dir), str(lance_path))
    assert lance_path.exists()
