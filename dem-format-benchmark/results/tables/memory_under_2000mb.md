# Combos with median memory < 2000 MiB

| Format         | Tool      | Query type   |   Median duration (ms) |   Median memory (MiB) |   Success rate |   # Runs |
|:---------------|:----------|:-------------|-----------------------:|----------------------:|---------------:|---------:|
| COG            | Rasterio  | Bbox window  |                 72.535 |               121.145 |              1 |       10 |
| Parquet (flat) | pandas    | Bbox window  |                114.95  |               207.88  |              1 |       10 |
| Parquet (flat) | DuckDB    | Bbox window  |                148.655 |               140.05  |              1 |       10 |
| COG            | rioxarray | Bbox window  |                179.855 |               143.035 |              1 |       10 |
| Parquet (flat) | Polars    | Bbox window  |                375.47  |               188.37  |              1 |       10 |
| COG            | Rasterio  | Point sample |                119.675 |               130.735 |              1 |       10 |
| COG            | rioxarray | Point sample |                183.085 |               143.405 |              1 |       10 |
| Parquet + S2   | Polars    | Point sample |                310.625 |               181.605 |              1 |       10 |
| Parquet (flat) | DuckDB    | Point sample |                462.925 |               146.26  |              1 |       10 |
| Parquet + S2   | DuckDB    | Point sample |               1558.91  |               255.51  |              1 |       10 |
| Parquet + S2   | pandas    | Point sample |               1660.22  |               847.995 |              1 |       10 |
| Parquet (flat) | Polars    | Point sample |               2266.01  |               915.635 |              1 |       10 |
| COG            | Rasterio  | Polygon      |                 73.245 |               122.245 |              1 |       10 |
| Parquet (flat) | pandas    | Polygon      |                 99.14  |               179.425 |              1 |       10 |
| Parquet (flat) | DuckDB    | Polygon      |                146.025 |               142.43  |              1 |       10 |
| Parquet (flat) | Polars    | Polygon      |                229.59  |               170.025 |              1 |       10 |
| COG            | rioxarray | Polygon      |              16048.8   |              1834.26  |              1 |       10 |
