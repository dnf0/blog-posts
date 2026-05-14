# Ubiquitous Language

This document defines the core domain terminology used across the DEM format benchmark project and blog post. Consistent usage of these terms ensures clarity and precision.

## Core Concepts

*   **Native 2D Hilbert Quadtree:** The specific spatial indexing strategy used to achieve maximum compression and spatial locality. It maps a 1D integer directly to the 2D `(col, row)` coordinates of a WGS84 1-arc-second grid, utilizing a Hilbert curve for ordering. Replaces generic terms like "Z-order", "Morton", "S2", or "H3".
*   **Coordinate Tax:** The massive file size bloat caused by explicitly storing `x` and `y` (or `lon` and `lat`) columns in a tabular format (like Parquet). Dropping these columns and relying solely on the Hilbert index eliminates this tax.
*   **Quadtree Compaction:** The process of merging four identical adjacent 30m child pixels into a single 60m parent cell (represented by a single Hilbert index) via bit-shifting. This acts as a 2D Run-Length Encoder.
*   **Vectorized Hash Join:** The execution method used by DuckDB and Polars to extract polygon data from Parquet files (`WHERE hilbert_index IN (...)`), which scales logarithmically and handles massive batch sizes exponentially faster than traditional raster processing.
*   **Sequential Raster Masking:** The traditional execution method used by GDAL/Rasterio (`rasterio.mask`), which involves looping over polygons one-by-one, fetching data, and burning bounding boxes into memory. It scales linearly.
*   **Client-Side Region Coverer:** The algorithm (now implemented as a pure Rust scanline rasterizer using `geo-rasterize` and `rayon`) that executes before the database query. It determines the exact pixel IDs that intersect a polygon and compacts them into a hierarchy, generating the exact `IN(...)` list.
*   **Hierarchical Compaction:** The recursive process of quadtree compaction. It does not stop at 60m; it continues up through 16 levels of the quadtree, merging 4 cells into 1 at each level, massively reducing file sizes for large uniform areas like lakes or flat plains.
*   **Serverless Rust Architecture:** The production deployment strategy that explicitly replaces GDAL's heavy C++ scanline rasterizer with a pure-Rust `geo-rasterize` pipeline. This sacrifices a fraction of a second in compute time to achieve a zero-dependency, horizontally scalable Web API.
*   **Dual-Pipeline Architecture:** The enterprise strategy of generating two separate Compacted Hilbert Parquet datasets: one in pure WGS84 (EPSG:4326) for mathematically perfect backend analytics, and a second in WebMercatorQuad (EPSG:3857) to serve directly to frontend web maps (Deck.gl/Mapbox), because the extreme compression makes storing both effectively free.