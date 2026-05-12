# VRT Assessment Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Python benchmarking harness to compare global VRT reads against Local VRT, GeoParquet, Kerchunk, Zarr, and STAC/TiTiler, and draft a blog post with the results.

**Architecture:** A Python package `vrt_benchmark` containing `data_gen.py` to procedurally generate synthetic COGs and indices, and `test_benchmark.py` using `pytest-benchmark` and `memray` for performance profiling.

**Tech Stack:** Python 3.12+, `rasterio`, `geopandas`, `zarr`, `fsspec`, `kerchunk`, `xarray`, `pytest-benchmark`, `memray`.

---

### Task 1: Project Setup & Dependencies

**Files:**
- Create: `vrt_benchmark/requirements.txt`
- Create: `vrt_benchmark/__init__.py`

- [ ] **Step 1: Write requirements**
```txt
pytest==8.2.0
pytest-benchmark==4.0.0
memray==1.12.0
rasterio==1.3.10
geopandas==0.14.4
zarr==2.17.2
fsspec==2024.3.1
kerchunk==0.2.6
xarray==2024.3.0
rioxarray==0.15.5
fastapi==0.111.0
uvicorn==0.29.0
httpx==0.27.0
```

- [ ] **Step 2: Create directory structure and install**
```bash
mkdir -p vrt_benchmark/data
touch vrt_benchmark/__init__.py
pip install -r vrt_benchmark/requirements.txt
```

- [ ] **Step 3: Commit**
```bash
git add vrt_benchmark/
git commit -m "build: setup project dependencies for VRT benchmark"
```

---

### Task 2: Data Generation - Base COGs

**Files:**
- Create: `vrt_benchmark/data_gen.py`
- Create: `vrt_benchmark/test_data_gen.py`

- [ ] **Step 1: Write failing test**
```python
# vrt_benchmark/test_data_gen.py
import os
from .data_gen import generate_cogs

def test_generate_cogs(tmp_path):
    output_dir = tmp_path / "cogs"
    output_dir.mkdir()
    generate_cogs(str(output_dir), grid_size=2)
    assert len(list(output_dir.glob("*.tif"))) == 4
```

- [ ] **Step 2: Run test to verify it fails**
```bash
pytest vrt_benchmark/test_data_gen.py -v
```

- [ ] **Step 3: Write minimal implementation**
```python
# vrt_benchmark/data_gen.py
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
```

- [ ] **Step 4: Run test to verify it passes**
```bash
pytest vrt_benchmark/test_data_gen.py -v
```

- [ ] **Step 5: Commit**
```bash
git add vrt_benchmark/data_gen.py vrt_benchmark/test_data_gen.py
git commit -m "feat: procedural COG generation"
```

---

### Task 3: Data Generation - Global VRT & GeoParquet

**Files:**
- Modify: `vrt_benchmark/data_gen.py`
- Modify: `vrt_benchmark/test_data_gen.py`

- [ ] **Step 1: Write failing test**
```python
# vrt_benchmark/test_data_gen.py (append)
from .data_gen import build_global_vrt, build_geoparquet
import geopandas as gpd

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
```

- [ ] **Step 2: Run test to verify it fails**
```bash
pytest vrt_benchmark/test_data_gen.py::test_build_indices -v
```

- [ ] **Step 3: Write minimal implementation**
```python
# vrt_benchmark/data_gen.py (append)
import glob
from rasterio.vrt import build_vrt
import geopandas as gpd
from shapely.geometry import box

def build_global_vrt(cog_dir: str, out_path: str):
    cogs = glob.glob(os.path.join(cog_dir, "*.tif"))
    with build_vrt(cogs) as vrt:
        # Save VRT XML
        with open(out_path, "w") as f:
            f.write(vrt.read().decode("utf-8") if hasattr(vrt.read(), "decode") else "")

def build_geoparquet(cog_dir: str, out_path: str):
    cogs = glob.glob(os.path.join(cog_dir, "*.tif"))
    records = []
    for cog in cogs:
        with rasterio.open(cog) as src:
            bounds = src.bounds
            records.append({
                "filepath": cog,
                "geometry": box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            })
    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf.to_parquet(out_path)
```
*(Self-Correction during implementation: `build_vrt` might not be right in rasterio, use gdalbuildvrt via subprocess if needed. For now, we will use a simple gdal wrapper or exact rasterio bounds).* Let's use exact implementation:
```python
# vrt_benchmark/data_gen.py (overwrite append)
import subprocess

def build_global_vrt(cog_dir: str, out_path: str):
    cogs = glob.glob(os.path.join(cog_dir, "*.tif"))
    subprocess.run(["gdalbuildvrt", out_path] + cogs, check=True)

def build_geoparquet(cog_dir: str, out_path: str):
    cogs = glob.glob(os.path.join(cog_dir, "*.tif"))
    records = []
    for cog in cogs:
        with rasterio.open(cog) as src:
            bounds = src.bounds
            records.append({
                "filepath": cog,
                "geometry": box(bounds.left, bounds.bottom, bounds.right, bounds.top)
            })
    gdf = gpd.GeoDataFrame(records, crs="EPSG:4326")
    gdf.to_parquet(out_path)
```

- [ ] **Step 4: Run test to verify it passes**
```bash
pytest vrt_benchmark/test_data_gen.py::test_build_indices -v
```

- [ ] **Step 5: Commit**
```bash
git add vrt_benchmark/data_gen.py vrt_benchmark/test_data_gen.py
git commit -m "feat: global VRT and GeoParquet generation"
```

---

### Task 4: Benchmarking Harness Setup

**Files:**
- Create: `vrt_benchmark/test_benchmark.py`

- [ ] **Step 1: Write setup fixture and Global VRT benchmark**
```python
# vrt_benchmark/test_benchmark.py
import pytest
import os
from .data_gen import generate_cogs, build_global_vrt, build_geoparquet
import rasterio
from rasterio.windows import from_bounds

@pytest.fixture(scope="session")
def dataset(tmp_path_factory):
    dir_path = tmp_path_factory.mktemp("data")
    cog_dir = dir_path / "cogs"
    generate_cogs(str(cog_dir), grid_size=10) # 100 COGs
    
    vrt_path = dir_path / "global.vrt"
    build_global_vrt(str(cog_dir), str(vrt_path))
    
    pq_path = dir_path / "index.parquet"
    build_geoparquet(str(cog_dir), str(pq_path))
    
    return {
        "vrt": str(vrt_path),
        "parquet": str(pq_path),
        "cogs": str(cog_dir)
    }

def test_bench_global_vrt(benchmark, dataset):
    # bounding box covering ~4 COGs in the middle
    bbox = (45, 45, 55, 55) 
    
    def run_read():
        with rasterio.open(dataset["vrt"]) as src:
            window = from_bounds(*bbox, transform=src.transform)
            return src.read(1, window=window)
            
    result = benchmark(run_read)
    assert result is not None
```

- [ ] **Step 2: Run benchmark to verify it passes**
```bash
pytest vrt_benchmark/test_benchmark.py::test_bench_global_vrt -v
```

- [ ] **Step 3: Commit**
```bash
git add vrt_benchmark/test_benchmark.py
git commit -m "bench: setup global VRT benchmark"
```

---

### Task 5: Benchmarking Local VRT

**Files:**
- Modify: `vrt_benchmark/test_benchmark.py`

- [ ] **Step 1: Write Local VRT Benchmark**
```python
# vrt_benchmark/test_benchmark.py (append)
import geopandas as gpd
from shapely.geometry import box
import tempfile
import subprocess

def test_bench_local_vrt(benchmark, dataset):
    bbox = (45, 45, 55, 55)
    search_geom = box(*bbox)
    
    def run_read():
        # Query index
        gdf = gpd.read_parquet(dataset["parquet"])
        intersecting = gdf[gdf.geometry.intersects(search_geom)]
        files = intersecting["filepath"].tolist()
        
        # Build local VRT dynamically
        with tempfile.NamedTemporaryFile(suffix=".vrt", delete=False) as tmp:
            local_vrt = tmp.name
            
        subprocess.run(["gdalbuildvrt", local_vrt] + files, check=True, capture_output=True)
        
        # Read from local VRT
        with rasterio.open(local_vrt) as src:
            window = from_bounds(*bbox, transform=src.transform)
            data = src.read(1, window=window)
            
        os.remove(local_vrt)
        return data

    result = benchmark(run_read)
    assert result is not None
```

- [ ] **Step 2: Run benchmark to verify it passes**
```bash
pytest vrt_benchmark/test_benchmark.py::test_bench_local_vrt -v
```

- [ ] **Step 3: Commit**
```bash
git add vrt_benchmark/test_benchmark.py
git commit -m "bench: add local VRT benchmark approach"
```

---

### Task 6: Draft Blog Post

**Files:**
- Create: `content/2026-05-12-why-vrts-fail.mdx` (at project root)

- [ ] **Step 1: Write initial blog post structure**
```bash
cat << 'EOF' > content/2026-05-12-why-vrts-fail.mdx
---
title: "Why Global VRTs Fail at Scale (And 5 Ways to Fix It)"
date: "2026-05-12"
---

# The Problem
Global VRTs degrade at scale due to XML parsing overhead and memory bloat.

# The 5 Alternatives
- Local VRTs
- GeoParquet Direct Reads
- Kerchunk
- Zarr
- STAC + TiTiler

# Benchmark Results
(Results will be pasted here after running memray and pytest-benchmark)

# Conclusion
Decision matrix on which architecture to use.
EOF
```

- [ ] **Step 2: Commit**
```bash
git add content/2026-05-12-why-vrts-fail.mdx
git commit -m "docs: draft blog post structure"
```
