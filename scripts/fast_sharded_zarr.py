import tensorstore as ts
import rasterio
import glob
import os
import asyncio
import time

async def build_sharded_zarr(cog_dir: str, out_path: str, grid_size: int = 10, tile_size: int = 100):
    total_size = grid_size * tile_size
    
    # We will make shards that hold 10x10 tiles (1000x1000 pixels)
    # The inner chunks will be 1 tile (100x100 pixels)
    shard_size = 1000
    if total_size < shard_size:
        shard_size = total_size
        
    spec = {
        "driver": "zarr3",
        "kvstore": {
            "driver": "file",
            "path": out_path
        },
        "metadata": {
            "shape": [total_size, total_size],
            "chunk_grid": {
                "name": "regular",
                "configuration": {"chunk_shape": [shard_size, shard_size]} # Shard shape
            },
            "data_type": "uint8",
            "codecs": [
                {
                    "name": "sharding_indexed",
                    "configuration": {
                        "chunk_shape": [tile_size, tile_size], # Inner chunk shape
                        "codecs": [
                             {"name": "blosc", "configuration": {"cname": "zstd", "clevel": 3, "shuffle": "bitshuffle", "typesize": 1}},
                        ],
                        "index_codecs": [
                            {"name": "bytes", "configuration": {"endian": "little"}},
                            {"name": "crc32c"}
                        ],
                        "index_location": "end"
                    }
                }
            ]
        },
        "create": True,
        "delete_existing": True
    }
    
    dataset = await ts.open(spec)
    
    async def write_cog(i, j, filename):
        with rasterio.open(filename) as src:
            data = src.read(1)
            
        x_start = i * tile_size
        x_end = x_start + tile_size
        y_start = j * tile_size
        y_end = y_start + tile_size
        
        # Tensorstore uses standard array indexing
        await dataset[x_start:x_end, y_start:y_end].write(data)
        
    tasks = []
    for i in range(grid_size):
        for j in range(grid_size):
            filename = os.path.join(cog_dir, f"cog_{i}_{j}.tif")
            if os.path.exists(filename):
                tasks.append(write_cog(i, j, filename))
                
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    # We need to add the parent dir to PYTHONPATH so we can import from vrt_benchmark
    import sys
    sys.path.append("/Users/danielfisher/repos/blog-posts")
    from vrt_benchmark.data_gen import generate_cogs
    
    # Generate mock COGs
    cog_dir = "data/cogs"
    out_zarr = "data/sharded.zarr"
    grid = 20 # 20x20 = 400 COGs (2000x2000 total pixels)
    
    print(f"Generating {grid*grid} mock COGs...")
    os.makedirs(cog_dir, exist_ok=True)
    generate_cogs(cog_dir, grid_size=grid)
    
    print("Writing Sharded Zarr using TensorStore...")
    start = time.time()
    asyncio.run(build_sharded_zarr(cog_dir, out_zarr, grid_size=grid, tile_size=100))
    end = time.time()
    print(f"Success! Built Sharded Zarr (V3) from {grid*grid} COGs in {end - start:.2f} seconds!")
