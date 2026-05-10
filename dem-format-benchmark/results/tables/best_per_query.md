# Best combo per query type

| Query type   | Fastest combo             | Lowest-memory combo        |
|:-------------|:--------------------------|:---------------------------|
| Bbox window  | COG / Rasterio (72.5 ms)  | COG / Rasterio (121.1 MiB) |
| Point sample | COG / Rasterio (119.7 ms) | COG / Rasterio (130.7 MiB) |
| Polygon      | COG / Rasterio (73.2 ms)  | COG / Rasterio (122.2 MiB) |
