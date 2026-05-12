# VRT Assessment & Blog Post Design Spec

## Project Overview
This project involves building a benchmarking harness to demonstrate why global Virtual Rasters (VRTs) are unsuited for massive global Cloud Optimized GeoTIFF (COG) datasets. The results will be published in a new technical blog post alongside 5 alternative architectural solutions.

## Data Setup & Scenario
To simulate a global dataset, we will procedurally generate a grid of small synthetic COGs (e.g., 100x100 pixels each). Using this dataset, we will build:
1. **Global VRT:** A standard XML VRT referencing all COGs.
2. **GeoParquet Index:** A spatial index containing the bounding boxes and file paths for all COGs.
3. **Kerchunk JSON:** A reference file mapping the chunks of all COGs globally.
4. **Zarr Archive:** A conversion of the COGs into a single Zarr group.
5. **STAC ItemCollection:** A mock metadata collection representing the COGs.

The benchmarking scenario involves a read operation targeting a specific bounding box that intersects 4 to 10 COGs out of the global set.

## Benchmarking Harness & Execution
The benchmarking suite will be implemented as a Python-native harness (`benchmark.py`) utilizing `memray` for memory profiling and standard time tracking for wall-clock duration.

Execution paths for the targeted bounding box read:
- **Global VRT:** Standard `rasterio` windowed read on the large VRT file.
- **Local VRT:** Spatial query against the GeoParquet index for intersecting COGs, dynamic generation of an in-memory (`/vsimem/`) VRT, followed by a `rasterio` read.
- **GeoParquet Direct:** Spatial query against the GeoParquet index, opening each intersecting COG individually via `rasterio`, and merging the data using `rasterio.merge`.
- **STAC + TiTiler:** Spin up a local FastAPI/TiTiler mock server, and benchmark an HTTP GET request to a `/mosaic/bbox` endpoint.
- **Kerchunk / RefFS:** Read using `fsspec` with the reference filesystem driver and `rioxarray` for the spatial slice.
- **Zarr:** Direct array slice using `zarr-python`.

## Blog Post Content & Structure
Title Concept: "Why Global VRTs Fail at Scale (And 5 Ways to Fix It)"

Structure:
1. **The Problem:** Explanation of how global VRTs degrade at scale due to XML parsing overhead, unbounded file handles, and memory bloat.
2. **The 5 Alternatives:** Technical overview of Local VRTs, GeoParquet direct reads, Kerchunk reference file systems, Zarr native multi-dimensional grids, and STAC/TiTiler dynamic mosaicking.
3. **The Benchmark Results:** Empirical data presenting the wall-clock performance and memory profiling results gathered from the Python harness.
4. **Decision Matrix:** A concluding guide on when an engineering team should choose which architecture based on specific project constraints.
