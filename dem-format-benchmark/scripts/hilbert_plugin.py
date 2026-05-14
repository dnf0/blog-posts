import polars as pl
from polars.plugins import register_plugin_function
from typing import Union, Any
from pathlib import Path
import polars_hilbert

def compute_hilbert(
    col: Union[str, pl.Expr],
    row: Union[str, pl.Expr],
) -> pl.Expr:
    plugin_path = Path(polars_hilbert.__file__).parent
    col_expr = pl.col(col) if isinstance(col, str) else col
    row_expr = pl.col(row) if isinstance(row, str) else row

    return register_plugin_function(
        args=[
            col_expr.cast(pl.UInt32),
            row_expr.cast(pl.UInt32),
        ],
        plugin_path=plugin_path,
        function_name="compute_hilbert",
        is_elementwise=True,
    )

def compact_hilbert(
    cells: Union[str, pl.Expr],
) -> pl.Expr:
    plugin_path = Path(polars_hilbert.__file__).parent
    cells_expr = pl.col(cells) if isinstance(cells, str) else cells

    return register_plugin_function(
        args=[cells_expr],
        plugin_path=plugin_path,
        function_name="compact_hilbert",
        is_elementwise=True,
    )

def hilbert_cells_for_polygons(polygons_geojson):
    exterior_rings_tuples = []
    for polygon_geojson in polygons_geojson:
        exterior_ring = polygon_geojson["coordinates"][0]
        exterior_rings_tuples.append([tuple(coord) for coord in exterior_ring])
    return polars_hilbert.hilbert_cells_for_polygons(exterior_rings_tuples)
