import polars as pl
import rasterio
import sys
from pathlib import Path
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS
from scripts.hilbert_plugin import compute_hilbert

cog_path = FORMAT_PATHS["cog"]["raw"]
with rasterio.open(cog_path) as src:
    transform = src.transform

q = (
    pl.scan_parquet("data/dem_base_raw.parquet")
    .with_columns([
        ((pl.col("band_value") / 2500).round() * 2500).cast(pl.Int16).alias("band_value"),
        (((transform.c + pl.col("col") * transform.a) + 180.0) * 3600.0).round().cast(pl.UInt32).alias("global_col"),
        ((90.0 - (transform.f + pl.col("row") * transform.e)) * 3600.0).round().cast(pl.UInt32).alias("global_row")
    ])
    .with_columns([
        compute_hilbert("global_col", "global_row").alias("z_index")
    ])
    .sort("z_index")
    .select(["band_value", "z_index"])
)
q.sink_parquet("data/test_uncompacted.parquet", compression="zstd")
import os
print("Uncompacted Size:", os.path.getsize("data/test_uncompacted.parquet") / (1024*1024), "MB")