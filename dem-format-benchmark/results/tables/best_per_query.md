# Best combo per query type

| Query type   | Fastest combo                           | Lowest-memory combo        |
|:-------------|:----------------------------------------|:---------------------------|
| Bbox window  | COG / Rasterio (75.3 ms)                | COG / Rasterio (127.0 MiB) |
| Point sample | COG / Rasterio (144.5 ms)               | Zarr / xarray (170.7 MiB)  |
| Polygon      | Zarr / Pure Rust Zarr (zarrs) (75.2 ms) | COG / Rasterio (134.1 MiB) |
