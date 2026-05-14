#!/bin/bash
set -e
echo "Cleaning memray..."
rm -rf results/memray/*
echo "Running make bench..."
uv run python scripts/benchmark.py
echo "Running make viz..."
uv run python scripts/visualize.py
echo "Done!"
