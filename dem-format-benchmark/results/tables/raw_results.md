# All results

| Format            | Tool                   | Query type   |   Median duration (ms) |   Median memory (MiB) |   Success rate |   # Runs |
|:------------------|:-----------------------|:-------------|-----------------------:|----------------------:|---------------:|---------:|
| COG               | Rasterio               | Bbox window  |                 75.265 |               127.005 |              1 |       12 |
| Parquet + Hilbert | Polars                 | Bbox window  |                141.925 |               225.34  |              1 |       12 |
| Parquet + Hilbert | DuckDB                 | Bbox window  |                265.77  |               179.415 |              1 |       12 |
| COG               | rioxarray              | Bbox window  |                515.595 |               171.5   |              1 |       12 |
| Zarr              | xarray                 | Bbox window  |                627.155 |               170.955 |              1 |       12 |
| Parquet (flat)    | Polars                 | Bbox window  |               1168.02  |               632.03  |              1 |       12 |
| Parquet (flat)    | DuckDB                 | Bbox window  |               4134.93  |               290.75  |              1 |       12 |
| COG               | Rasterio               | Point sample |                144.455 |               175.875 |              1 |       12 |
| Parquet + Hilbert | DuckDB                 | Point sample |                191.695 |               173.47  |              1 |       12 |
| Parquet + Hilbert | Polars                 | Point sample |                201.445 |               247.765 |              1 |       12 |
| COG               | rioxarray              | Point sample |                542.29  |               172     |              1 |       12 |
| Zarr              | xarray                 | Point sample |                676.005 |               170.71  |              1 |       12 |
| Parquet (flat)    | Polars                 | Point sample |               3535.11  |               668.51  |              1 |       12 |
| Zarr              | Pure Rust Zarr (zarrs) | Polygon      |                 75.175 |               135.4   |              1 |       12 |
| COG               | Rasterio               | Polygon      |                 90.235 |               134.085 |              1 |       12 |
| Parquet + Hilbert | Polars                 | Polygon      |                156.79  |               586.11  |              1 |       12 |
| Zarr              | Native Zarr            | Polygon      |                251.975 |               140.375 |              1 |       12 |
| Parquet + Hilbert | DuckDB                 | Polygon      |                452.69  |               223.5   |              1 |       12 |
| Lance             | DuckDB                 | Polygon      |               1358.54  |               383.45  |              1 |        9 |
| Parquet (flat)    | Polars                 | Polygon      |              11365     |               646.76  |              1 |       12 |
| Zarr              | xarray                 | Polygon      |              12765.3   |              3768.49  |              1 |       12 |
