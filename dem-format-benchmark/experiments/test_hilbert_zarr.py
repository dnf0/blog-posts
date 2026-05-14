import zarr
import numcodecs
import rioxarray
import numpy as np
import time
from pathlib import Path
import shutil

sys_path_added = False
import sys
sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS
from scripts.hilbert_plugin import compute_hilbert

cog_path = FORMAT_PATHS["cog"]["q2500"]
da = rioxarray.open_rasterio(cog_path).squeeze()
print("Loaded COG:", da.shape)

# Flatten
flat_data = da.values.flatten()
rows, cols = np.indices(da.shape)
global_cols = cols.flatten() + 8964 # Approximate offset from previous scripts
global_rows = rows.flatten() + 12600

# Compute Hilbert
print("Computing Hilbert...")
import polars as pl
df = pl.DataFrame({"global_col": global_cols.astype(np.uint32), "global_row": global_rows.astype(np.uint32), "band": flat_data.astype(np.int16)})
df = df.with_columns([compute_hilbert("global_col", "global_row").alias("z_index")])

# Sort by Hilbert
print("Sorting...")
df = df.sort("z_index")
ordered_data = df["band"].to_numpy()

out_path = "data/dem_hilbert_1d.zarr"
if Path(out_path).exists():
    shutil.rmtree(out_path)

compressor = numcodecs.Blosc(cname='zstd', clevel=9, shuffle=numcodecs.Blosc.BITSHUFFLE)
z = zarr.create(store=out_path, shape=ordered_data.shape, chunks=(512*512,), dtype='i2', zarr_format=2, compressor=compressor)
z[:] = ordered_data

import subprocess
size = subprocess.check_output(f"du -sh {out_path}", shell=True).decode().split()[0]
print(f"1D Hilbert Zarr Size: {size}")
