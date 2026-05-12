import os
import rasterio
from rasterio.transform import from_origin
import numpy as np

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
