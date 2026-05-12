import os
from .data_gen import generate_cogs

def test_generate_cogs(tmp_path):
    output_dir = tmp_path / "cogs"
    output_dir.mkdir()
    generate_cogs(str(output_dir), grid_size=2)
    assert len(list(output_dir.glob("*.tif"))) == 4
