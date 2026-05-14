import zarr
import numcodecs
import rioxarray
from pathlib import Path

cog_path = "data/dem_cog_q2500.tif"
da = rioxarray.open_rasterio(cog_path, chunks={"x": 512, "y": 512})

import shutil
if Path("data/test_zarr_zstd.zarr").exists():
    shutil.rmtree("data/test_zarr_zstd.zarr")

compressor = numcodecs.Blosc(cname='zstd', clevel=9, shuffle=numcodecs.Blosc.BITSHUFFLE)
da.to_zarr("data/test_zarr_zstd.zarr", mode="w", compute=True, encoding={da.name or 'band_data': {'compressor': compressor}})

if Path("data/test_zarr_lzma.zarr").exists():
    shutil.rmtree("data/test_zarr_lzma.zarr")

compressor = numcodecs.LZMA(preset=9)
da.to_zarr("data/test_zarr_lzma.zarr", mode="w", compute=True, encoding={da.name or 'band_data': {'compressor': compressor}})

import subprocess
import json
print("Default Zarr size (MB):", subprocess.check_output("du -sh data/dem_q2500.zarr", shell=True).decode().split()[0])
print("Zstd+Bitshuffle Zarr size (MB):", subprocess.check_output("du -sh data/test_zarr_zstd.zarr", shell=True).decode().split()[0])
print("LZMA Zarr size (MB):", subprocess.check_output("du -sh data/test_zarr_lzma.zarr", shell=True).decode().split()[0])
