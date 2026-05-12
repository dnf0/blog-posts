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
