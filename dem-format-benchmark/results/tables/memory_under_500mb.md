# Combos with median memory < 500 MiB

| Format            | Tool      | Query type   |   Median duration (ms) |   Median memory (MiB) |   Success rate |   # Runs |
|:------------------|:----------|:-------------|-----------------------:|----------------------:|---------------:|---------:|
| COG               | Rasterio  | Bbox window  |                 71.825 |               126.225 |              1 |       12 |
| COG               | rioxarray | Bbox window  |                488.215 |               171.065 |              1 |       12 |
| Parquet (flat)    | DuckDB    | Bbox window  |               4096.69  |               287.945 |              1 |       12 |
| COG               | Rasterio  | Point sample |                137.315 |               175.04  |              1 |       12 |
| Parquet + Z-order | DuckDB    | Point sample |                177.325 |               172.94  |              1 |       12 |
| Parquet + Z-order | Polars    | Point sample |                205.07  |               244.765 |              1 |       12 |
| COG               | rioxarray | Point sample |                526.775 |               170.93  |              1 |       12 |
| COG               | Rasterio  | Polygon      |                 71.14  |               127.15  |              1 |       12 |
| Parquet (flat)    | DuckDB    | Polygon      |               4140.07  |               294.115 |              1 |       12 |
