# Design Spec: High-Concurrency Global Hazard Data Architecture Blog Post

## Overview
A technical deep-dive aimed at Data and Platform Engineers, explaining the architectural evolution required to synchronously query massive polygon batches against global, multi-dimensional hazard datasets. The post uses a localized DEM proxy to benchmark standard GIS tools against relational joins and native chunked arrays, ultimately identifying a pure-Rust Zarr backend as the optimal production architecture.

## Target Audience
Data Engineers and Platform Architects building highly concurrent, serverless geospatial APIs. The tone is professional, objective, and analytical, avoiding hype or aggrandizement.

## Section Outline

### 1. The Global Problem Context
*   **The Reality:** The production requirement is a global 30m dataset over land, characterized by long run-lengths (smooth climate/hazard data).
*   **The Dimensionality:** The data is hyper-dimensional: 12 years × 4 scenarios × 5 return periods × 3 percentiles = 720 distinct layers per spatial pixel.
*   **The Constraint:** The API must execute synchronous, low-latency extractions for batches of up to 100,000 irregular polygons.
*   **The Proxy Benchmark:** To empirically test architectures, we use a 6°×6° Copernicus DEM (1.7GB, ~466M pixels) quantized to `q2500`. This perfectly simulates the smooth, blocky nature of the global hazard data while allowing rigorous, reproducible micro-benchmarking.

### 2. The Baseline: Sequential Raster Masking
*   **The Approach:** Standard GDAL/Rasterio pipeline against a Cloud Optimized GeoTIFF (COG).
*   **The Result:** The COG compresses well (9.7 MB), but sequential raster masking is an $O(N)$ operation over geometries.
*   **The Bottleneck:** Extracting 100,000 polygons via Rasterio takes ~181 seconds (over 3 minutes). While excellent for single point lookups or notebooks, this linear scaling fails synchronous Web API constraints.

### 3. The Relational Pivot: Tabular Joins & Quadtree Compaction
*   **The Approach:** Converting the spatial problem into a relational database problem by utilizing a Native 2D Hilbert Curve and dropping explicit coordinates to save space.
*   **Quadtree Compaction:** Because the hazard data proxy is smooth, identical 30m child pixels are recursively merged into larger parent cells (up to 16 levels).
*   **The Results:**
    *   The Parquet file shrinks dramatically to 2.8 MB.
    *   The Database Query (the $O(\log N)$ Hash Join) processes 100,000 polygons in 11–19 seconds.
    *   However, the geometric preprocessing (the "Covering" step) still scales linearly, dragging total execution time to ~40 seconds.

### 4. The Area-Weighting Trade-off
*   **The Limitation:** Quadtree Compaction fundamentally alters `AVG()` aggregations. A large, compacted flat area is reduced to a single row, skewing standard SQL averages when combined with uncompacted noisy edge pixels. 
*   **The Analytical Solution:** 
    *   For `MAX()` or `MIN()` queries (e.g., peak flood depth), the 2.8 MB Compacted Parquet is mathematically robust. 
    *   For rigorous `AVG()` polygon analytics, an Uncompacted Hilbert Parquet is required (which compresses to ~319 MB via Parquet's native ZSTD/RLE), ensuring every pixel retains equal weight.

### 5. Native Zarr Integration
*   **The Ecosystem Assessment:** Zarr provides excellent compression (9.8 MB), but standard Python bindings (`xarray` / `rioxarray`) introduce significant overhead for surgical polygon masking, failing at 110s for just 100 polygons.
*   **The Rust Implementation:** Bypassing the Python GIL, we implement a custom PyO3 plugin using the pure-Rust `zarrs` crate, `geo-rasterize`, and `rayon` for parallel processing.
*   **The Performance:** Operating directly on the raw chunked bytes natively in Rust, the pipeline extracts 100,000 polygons in **1.57 seconds**. Because it operates on raw pixels, it maintains GDAL-level mathematical accuracy for area-weighting without the Parquet boundary paradox.

### 6. Scaling Back to the Globe (Conclusion)
*   **Applying the Lessons:** Returning to the original multi-dimensional problem (720 layers per pixel).
*   **Why Rust+Zarr Wins at Scale:** Parquet schemas struggle with 720-layer hyper-dimensionality, whereas Zarr natively handles N-dimensional chunking (`[layer, y, x]`). 
*   **Storage Constraints:** Addressing the Zarr v2 "many files" problem by stipulating Zarr v3 Sharding for cloud object storage.
*   **Final Verdict:** A pure-Rust backend querying sharded N-dimensional Zarr arrays provides the optimal balance of storage compression, analytical accuracy, and sub-second synchronous API performance for global hazard data.