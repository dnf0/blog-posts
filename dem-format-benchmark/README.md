# DEM Format Benchmark

Compares reading DEM data from COG vs Parquet (with S2/H3 indexing) vs GeoParquet
on constrained hardware (M1 Mac, 8GB RAM).

## Quick Start

```bash
cd dem-format-benchmark
pip install -e .

# Stage 1: Download data (run once, ~10 min)
python scripts/data_prep.py

# Stage 2: Run benchmarks (run once, ~20 min)
python scripts/benchmark.py

# Stage 3: Generate visualizations (fast, iterate freely)
python scripts/visualize.py

# Stage 4: Generate blog post (fast)
python scripts/generate_post.py
```

## Configuration

Edit `config.py` to change:
- `REGION_BOUNDS` — geographic extent
- `S2_LEVEL` / `H3_RESOLUTION` — spatial index granularity
- `NUM_RUNS` — repetitions per benchmark
- `BENCHMARK_COMBOS` — which format×tool×query combos to test

## Output

- `results/benchmarks.parquet` — raw timing + memory data
- `plots/` — publication-quality PNGs
- `results/tables/` — markdown table snippets
- `content/YYYY-MM-DD-dem-format-benchmark.mdx` — final blog post

## Adding a new format or tool

1. Add the file path to `config.FORMAT_PATHS`
2. Add the query logic to `benchmark.py` (`_execute_benchmark` dispatch)
3. Add entries to `config.BENCHMARK_COMBOS`
4. Add label to `config.FORMAT_LABELS` or `config.TOOL_LABELS`
