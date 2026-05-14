import polars as pl
from polars.plugins import register_plugin_function
from typing import Union, Any
from pathlib import Path
import polars_s2

def pixel_to_cells(
    col: Union[str, pl.Expr],
    row: Union[str, pl.Expr],
    transform: Any,
    level: Union[int, pl.Expr] = 18
) -> pl.Expr:
    """
    Convert pixel coordinates (col, row) and an affine transform to a list of S2 cell IDs 
    that fully cover the pixel bounding box.
    """
    plugin_path = Path(polars_s2.__file__).parent
    col_expr = pl.col(col) if isinstance(col, str) else col
    row_expr = pl.col(row) if isinstance(row, str) else row

    return register_plugin_function(
        args=[
            col_expr.cast(pl.UInt32),
            row_expr.cast(pl.UInt32),
            (pl.lit(transform.a) if isinstance(transform.a, (int, float)) else transform.a).cast(pl.Float64),
            (pl.lit(transform.b) if isinstance(transform.b, (int, float)) else transform.b).cast(pl.Float64),
            (pl.lit(transform.c) if isinstance(transform.c, (int, float)) else transform.c).cast(pl.Float64),
            (pl.lit(transform.d) if isinstance(transform.d, (int, float)) else transform.d).cast(pl.Float64),
            (pl.lit(transform.e) if isinstance(transform.e, (int, float)) else transform.e).cast(pl.Float64),
            (pl.lit(transform.f) if isinstance(transform.f, (int, float)) else transform.f).cast(pl.Float64),
            (pl.lit(level) if isinstance(level, int) else level).cast(pl.UInt32),
        ],
        plugin_path=plugin_path,
        function_name="pixel_to_cells",
        is_elementwise=True,
    )

def lat_lon_to_cell(
    lon: Union[str, pl.Expr],
    lat: Union[str, pl.Expr],
    transform: Any,
    level: Union[int, pl.Expr] = 18
) -> pl.Expr:
    """
    Convert lat/lon to S2 cell IDs by reversing the transform into col/row
    and using pixel_to_cell.
    """
    lon_expr = pl.col(lon) if isinstance(lon, str) else lon
    lat_expr = pl.col(lat) if isinstance(lat, str) else lat

    # Reverse the transform for a non-rotated raster (b=0, d=0)
    col_expr = ((lon_expr - transform.c) / transform.a - 0.5).round().cast(pl.UInt32)
    row_expr = ((lat_expr - transform.f) / transform.e - 0.5).round().cast(pl.UInt32)

    return pixel_to_cell(col_expr, row_expr, transform, level)

def compact_cells(
    cells: Union[str, pl.Expr],
) -> pl.Expr:
    """
    Compact a list of S2 cell IDs into their normalized union representation.
    """
    plugin_path = Path(polars_s2.__file__).parent
    cells_expr = pl.col(cells) if isinstance(cells, str) else cells

    return register_plugin_function(
        args=[cells_expr],
        plugin_path=plugin_path,
        function_name="compact_cells",
        is_elementwise=True,
    )

