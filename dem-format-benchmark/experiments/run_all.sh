#!/bin/bash
set -e
echo "Cleaning data directory..."
rm -rf data/*.parquet data/*.geoparquet data/*.zarr data/dem_cog_q*.tif
echo "Running make data..."
uv run python scripts/data_prep.py
echo "Running make bench..."
uv run python scripts/benchmark.py
echo "Running crossover analysis..."
uv run python scripts/crossover_analysis.py
echo "Running make viz..."
uv run python scripts/visualize.py
echo "Done!"
