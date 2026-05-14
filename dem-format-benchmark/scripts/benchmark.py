"""Stage 2: Run format x tool x query_type benchmarks with memray profiling.

Each combination is executed inside a subprocess (for timeout/isolation),
wrapped by memray.Tracker. Results are collected and saved as Parquet.
"""

import multiprocessing
import subprocess
import sys
import time
import traceback
from pathlib import Path

import pandas as pd
import psutil
from shapely.geometry import shape, box as shapely_box

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (  # noqa: E402
    BENCHMARK_COMBOS,
    DATA_VARIANTS,
    FORMAT_LABELS,
    FORMAT_PATHS,
    H3_RESOLUTION,
    MEMRAY_DIR,
    NUM_RUNS,
    RESULTS_DIR,
    S2_LEVEL,
    generate_query_polygons,
    TIMEOUT_SECONDS,
    TOOL_LABELS,
    VARIANT_LABELS,
    generate_query_bboxes,
    generate_query_points,
    get_path,
)

# ---------------------------------------------------------------------------
# Pre-computed query datasets (materialised once at module level).
# In spawned subprocesses the module is re-imported, so each child gets fresh
# copies without shared-memory side-effects.
# ---------------------------------------------------------------------------
QUERY_POINTS = generate_query_points()          # list[tuple[float, float]]
QUERY_BBOXES = generate_query_bboxes()          # list[tuple[float,float,float,float]]
QUERY_POLYGONS = generate_query_polygons()      # list[dict]

# ---------------------------------------------------------------------------
# Import availability check
# ---------------------------------------------------------------------------


def check_imports() -> set[str]:
    """Return the set of tool keys whose Python packages are importable."""
    available: set[str] = set()
    tool_imports: dict[str, str] = {
        "rasterio": "rasterio",
        "rioxarray": "rioxarray",
        "duckdb": "duckdb",
        "polars": "polars",
        "s2sphere": "s2sphere",
        "h3": "h3",
        "xarray": "xarray",
        "zarr_native": "zarr",
        "zarrs_rust": "zarrs_plugin",
    }
    for tool, module in tool_imports.items():
        try:
            __import__(module)
            available.add(tool)
        except ImportError:
            pass
    return available


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _geojson_to_wkt(geojson_dict: dict) -> str:
    """Convert a GeoJSON geometry dict to a WKT string."""
    geom = shape(geojson_dict)
    return geom.wkt


def _polygon_bbox(polygon_geojson: dict) -> tuple[float, float, float, float]:
    """Return (minx, miny, maxx, maxy) for a GeoJSON polygon."""
    geom = shape(polygon_geojson)
    return geom.bounds  # type: ignore[return-value]


def _filesize_mb(format_key: str, variant: str = "raw") -> float:
    """On-disk size of a format file in MiB."""
    path = get_path(format_key, variant)
    if path and path.exists():
        return round(path.stat().st_size / (1024 * 1024), 2)
    return 0.0


# ---------------------------------------------------------------------------
# Query implementations
# ---------------------------------------------------------------------------


def _query_cog(tool_key: str, query_type: str, variant: str) -> None:
    """Execute a spatial query against the COG file."""
    cog = str(get_path("cog", variant))

    if tool_key == "rasterio":
        import rasterio
        from rasterio import mask as rast_mask

        if query_type == "point":
            with rasterio.open(cog) as src:
                # Batch read all 100 points simultaneously
                pts = QUERY_POINTS
                # .sample() takes an iterable of (lon, lat) pairs
                for val in src.sample(pts):
                    _ = val
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            with rasterio.open(cog) as src:
                from rasterio.windows import from_bounds
                window = from_bounds(*bbox, src.transform)
                _ = src.read(1, window=window)
        elif query_type == "polygon":
            with rasterio.open(cog) as src:
                for poly_geojson in QUERY_POLYGONS:
                    poly = shape(poly_geojson)
                    masked, _ = rast_mask.mask(src, [poly], crop=True)
                    _ = masked

    elif tool_key == "rioxarray":
        import rioxarray  # noqa: F401 — registers .rio accessor
        import xarray as xr

        if query_type == "point":
            ds = xr.open_dataset(cog, engine="rasterio")
            pts = QUERY_POINTS
            for lon, lat in pts:
                _ = ds.sel(x=lon, y=lat, method="nearest")
            ds.close()
        elif query_type == "bbox":
            ds = xr.open_dataset(cog, engine="rasterio")
            bbox = QUERY_BBOXES[0]
            _ = ds.sel(
                x=slice(bbox[0], bbox[2]),
                y=slice(bbox[3], bbox[1]),
            )
            ds.close()
        elif query_type == "polygon":
            ds = xr.open_dataset(cog, engine="rasterio")
            for poly_geojson in QUERY_POLYGONS:
                poly = shape(poly_geojson)
                clipped = ds.rio.clip([poly.__geo_interface__], ds.rio.crs)
                _ = clipped
            ds.close()

    elif tool_key == "duckdb":
        import os
        import rasterio as _rio
        _gdal_data = os.path.join(os.path.dirname(_rio.__file__), 'gdal_data')
        if os.path.isdir(_gdal_data):
            os.environ.setdefault('GDAL_DATA', _gdal_data)

        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")
        

        if query_type == "point":
            pts = QUERY_POINTS
            for lon, lat in pts:
                con.execute(
                    """SELECT (ST_DumpAsPolygons(rast)).val
                       FROM ST_Read(?) AS rast
                       WHERE ST_Contains(
                           ST_MakeEnvelope(ST_MinX(rast.rast_bounds),
                                           ST_MinY(rast.rast_bounds),
                                           ST_MaxX(rast.rast_bounds),
                                           ST_MaxY(rast.rast_bounds)),
                           ST_MakePoint(?, ?)
                       )""",
                    [cog, lon, lat],
                ).fetchall()
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            con.execute(
                """SELECT (ST_DumpAsPolygons(rast)).val
                   FROM ST_Read(?) AS rast
                   WHERE ST_Intersects(
                       ST_MakeEnvelope(
                           ST_MinX(rast.rast_bounds),
                           ST_MinY(rast.rast_bounds),
                           ST_MaxX(rast.rast_bounds),
                           ST_MaxY(rast.rast_bounds)
                       ),
                       ST_MakeEnvelope(?, ?, ?, ?)
                   )""",
                [cog, bbox[0], bbox[1], bbox[2], bbox[3]],
            ).fetchall()
        elif query_type == "polygon":
            for poly_geojson in QUERY_POLYGONS:
                wkt_poly = _geojson_to_wkt(poly_geojson)
                con.execute(
                    """SELECT (ST_DumpAsPolygons(rast)).val
                       FROM ST_Read(?) AS rast
                       WHERE ST_Intersects(
                           ST_MakeEnvelope(
                               ST_MinX(rast.rast_bounds),
                               ST_MinY(rast.rast_bounds),
                               ST_MaxX(rast.rast_bounds),
                               ST_MaxY(rast.rast_bounds)
                           ),
                           ST_GeomFromText(?)
                       )""",
                    [cog, wkt_poly],
                ).fetchall()
        con.close()


def _query_parquet(format_key: str, tool_key: str, query_type: str, variant: str) -> None:
    """Execute a spatial query against a (flat | S2 | H3 | Z-order) Parquet file."""
    path = str(get_path(format_key, variant))
    is_s2 = format_key == "parquet_s2"
    is_h3 = format_key == "parquet_h3"
    is_hilbert = format_key == "parquet_hilbert"
    has_cell = is_s2 or is_h3 or is_hilbert

    def _hilbert_hierarchy_for_point(lon, lat):
        import rasterio
        with rasterio.open(FORMAT_PATHS["cog"]["raw"]) as src:
            transform = src.transform
        global_col = round(((transform.c + lon * transform.a) + 180.0) * 3600.0)
        global_row = round(((90.0 - (transform.f + lat * transform.e)) * 3600.0))
        x = int(global_col)
        y = int(global_row)
        d = 0
        for i in range(31, -1, -1):
            rx = (x >> i) & 1
            ry = (y >> i) & 1
            quad = (3 * rx) ^ ry
            d += quad << (2 * i)
            if ry == 0:
                if rx == 1:
                    mask = (1 << (i + 1)) - 1 if i < 31 else 0xFFFFFFFF
                    x = x ^ mask
                    y = y ^ mask
                x, y = y, x
        hierarchy = []
        for lvl in range(16):
            hierarchy.append((d >> (2 * lvl)) << 4 | lvl)
        return hierarchy

    # Helper: pre-compute cell IDs covering a bbox / polygon
    def _s2_cells_for_bbox(bbox):
        import s2sphere as s2
        rect = s2.LatLngRect.from_point_pair(
            s2.LatLng.from_degrees(bbox[1], bbox[0]),
            s2.LatLng.from_degrees(bbox[3], bbox[2]),
        )
        coverer = s2.RegionCoverer()
        coverer.min_level = S2_LEVEL
        coverer.max_level = S2_LEVEL
        coverer.max_cells = 500
        return [c.id() for c in coverer.get_covering(rect)]

    def _s2_cells_for_polygon(polygon_geojson):
        import s2sphere as s2
        b = _polygon_bbox(polygon_geojson)
        rect = s2.LatLngRect.from_point_pair(
            s2.LatLng.from_degrees(b[1], b[0]),
            s2.LatLng.from_degrees(b[3], b[2]),
        )
        coverer = s2.RegionCoverer()
        coverer.min_level = S2_LEVEL
        coverer.max_level = S2_LEVEL
        coverer.max_cells = 500
        return [c.id() for c in coverer.get_covering(rect)]

    def _s2_hierarchy_for_point(lon, lat):
        import s2sphere
        cell = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon))
        return [cell.parent(lvl).id() for lvl in range(1, S2_LEVEL + 1)]

    def _h3_cells_for_bbox(bbox):
        import h3.api.numpy_int as h3
        poly = shapely_box(bbox[0], bbox[1], bbox[2], bbox[3])
        return h3.polygon_to_cells(
            h3.geo_to_h3shape(poly.__geo_interface__), H3_RESOLUTION,
        )

    def _h3_cells_for_polygon(polygon_geojson):
        import h3.api.numpy_int as h3
        return h3.polygon_to_cells(
            h3.geo_to_h3shape(polygon_geojson), H3_RESOLUTION,
        )

    def _h3_hierarchy_for_point(lon, lat):
        import h3.api.numpy_int as h3
        cell = h3.latlng_to_cell(lat, lon, H3_RESOLUTION)
        return [h3.cell_to_parent(cell, res) for res in range(0, H3_RESOLUTION + 1)]

    def _hilbert_cells_for_bbox(bbox):
        col_min = int(round((bbox[0] + 180.0) * 3600.0))
        col_max = int(round((bbox[2] + 180.0) * 3600.0))
        row_max = int(round((90.0 - bbox[1]) * 3600.0))
        row_min = int(round((90.0 - bbox[3]) * 3600.0))
        if col_min > col_max: col_min, col_max = col_max, col_min
        if row_min > row_max: row_min, row_max = row_max, row_min
        
        def hilbert_encode(x, y):
            d = 0
            for i in range(31, -1, -1):
                rx = (x >> i) & 1
                ry = (y >> i) & 1
                quad = (3 * rx) ^ ry
                d += quad << (2 * i)
                if ry == 0:
                    if rx == 1:
                        mask = 0xFFFFFFFF if i == 31 else (1 << (i + 1)) - 1
                        x = x ^ mask
                        y = y ^ mask
                    x, y = y, x
            return d
            
        expanded = set()
        for c in range(col_min, col_max + 1):
            for r in range(row_min, row_max + 1):
                d = hilbert_encode(c, r)
                for lvl in range(16):
                    expanded.add((d >> (2 * lvl)) << 4 | lvl)
        return list(expanded)

    def _hilbert_cells_for_polygons(polygons_geojson):
        import sys
        from pathlib import Path
        scripts_dir = str(Path(__file__).resolve().parent)
        if scripts_dir not in sys.path:
            sys.path.append(scripts_dir)
        from hilbert_plugin import hilbert_cells_for_polygons as r_coverer
        return r_coverer(polygons_geojson)

    # Pre-compute cell ID lists for S2/H3 queries
    cell_ids_before = None
    if has_cell and query_type == "bbox":
        bbox = QUERY_BBOXES[0]
        if is_s2:
            covering = _s2_cells_for_bbox(bbox)
            expanded = set()
            import s2sphere
            for cid in covering:
                cell = s2sphere.CellId(cid)
                for lvl in range(1, S2_LEVEL + 1):
                    expanded.add(cell.parent(lvl).id())
            cell_ids_before = list(expanded)
        elif is_h3:
            covering = _h3_cells_for_bbox(bbox)
            expanded = set()
            import h3.api.numpy_int as h3
            for cid in covering:
                for res in range(0, H3_RESOLUTION + 1):
                    expanded.add(h3.cell_to_parent(cid, res))
            cell_ids_before = list(expanded)
        elif is_hilbert:
            cell_ids_before = _hilbert_cells_for_bbox(bbox)
    elif has_cell and query_type == "polygon":
        if is_hilbert:
            cell_ids_before_list = [_hilbert_cells_for_polygons(QUERY_POLYGONS)]
        else:
            cell_ids_before_list = []
            for poly_geojson in QUERY_POLYGONS:
                if is_s2:
                    covering = _s2_cells_for_polygon(poly_geojson)
                    expanded = set()
                    import s2sphere
                    for cid in covering:
                        cell = s2sphere.CellId(cid)
                        for lvl in range(1, S2_LEVEL + 1):
                            expanded.add(cell.parent(lvl).id())
                    cell_ids_before_list.append(list(expanded))
                elif is_h3:
                    pass

    if tool_key == "duckdb":
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        if query_type == "point":
            pts = QUERY_POINTS[:10]
            if is_s2:
                cell_ids = []
                for lon, lat in pts:
                    cell_ids.extend(_s2_hierarchy_for_point(lon, lat))
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE s2_cell IN ({','.join(map(str, cell_ids))})"
                ).fetchall()
            elif is_h3:
                cell_ids = []
                for lon, lat in pts:
                    cell_ids.extend(_h3_hierarchy_for_point(lon, lat))
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE h3_cell IN ({','.join(map(str, cell_ids))})"
                ).fetchall()
            elif is_hilbert:
                cell_ids = []
                for lon, lat in pts:
                    cell_ids.extend(_hilbert_hierarchy_for_point(lon, lat))
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE z_index IN ({','.join(map(str, cell_ids))})"
                ).fetchall()
            else:
                for lon, lat in pts:
                    con.execute(
                        f"SELECT band_value FROM '{path}' WHERE x = ? AND y = ?",
                        [lon, lat],
                    ).fetchall()
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            if is_s2:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE s2_cell IN ({','.join(map(str, cell_ids_before))})"
                ).fetchall()
            elif is_h3:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE h3_cell IN ({','.join(map(str, cell_ids_before))})"
                ).fetchall()
            elif is_hilbert:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE z_index IN (SELECT * FROM UNNEST(?))",
                    [cell_ids_before],
                ).fetchall()
            else:
                con.execute(
                    f"SELECT band_value FROM '{path}' WHERE x >= ? AND x <= ? AND y >= ? AND y <= ?",
                    [bbox[0], bbox[2], bbox[1], bbox[3]],
                ).fetchall()
        elif query_type == "polygon":
            if is_s2:
                all_cids = [str(cid) for cids in cell_ids_before_list for cid in cids]
                if all_cids:
                    con.execute(f"SELECT band_value FROM '{path}' WHERE s2_cell IN ({','.join(all_cids)})").fetchall()
            elif is_h3:
                pass
            elif is_hilbert:
                all_cids = [int(cid) for cids in cell_ids_before_list for cid in cids]
                if all_cids:
                    unique_cids = list(set(all_cids))
                    import pyarrow as pa
                    arrow_table = pa.table({'z_index': pa.array(unique_cids, type=pa.uint64())})
                    con.register('cids_table', arrow_table)
                    con.execute(f"SELECT band_value FROM '{path}' INNER JOIN cids_table USING (z_index)").fetchall()
            else:
                for poly_json in QUERY_POLYGONS:
                    pb = _polygon_bbox(poly_json)
                    con.execute(
                        f"SELECT band_value FROM '{path}' WHERE x >= ? AND x <= ? AND y >= ? AND y <= ?",
                        [pb[0], pb[2], pb[1], pb[3]],
                    ).fetchall()
        con.close()

    elif tool_key == "polars":
        import polars as pl

        if query_type == "point":
            pts = QUERY_POINTS[:10]
            df = pl.scan_parquet(path)
            if is_s2:
                cell_ids = []
                for lon, lat in pts:
                    cell_ids.extend(_s2_hierarchy_for_point(lon, lat))
                result = df.filter(pl.col("s2_cell").is_in(cell_ids)).collect()
            elif is_h3:
                cell_ids = []
                for lon, lat in pts:
                    cell_ids.extend(_h3_hierarchy_for_point(lon, lat))
                result = df.filter(pl.col("h3_cell").is_in(cell_ids)).collect()
            elif is_hilbert:
                cell_ids = []
                for lon, lat in pts:
                    cell_ids.extend(_hilbert_hierarchy_for_point(lon, lat))
                result = df.filter(pl.col("z_index").is_in(cell_ids)).collect()
            else:
                expr = pl.lit(False)
                for lon, lat in pts:
                    expr = expr | ((pl.col("x") == lon) & (pl.col("y") == lat))
                result = df.filter(expr).collect()
            _ = result
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            df = pl.scan_parquet(path)
            if is_s2:
                result = df.filter(pl.col("s2_cell").is_in(cell_ids_before)).collect()
            elif is_h3:
                result = df.filter(pl.col("h3_cell").is_in(cell_ids_before)).collect()
            elif is_hilbert:
                result = df.filter(pl.col("z_index").is_in(cell_ids_before)).collect()
            else:
                result = df.filter(
                    (pl.col("x") >= bbox[0])
                    & (pl.col("x") <= bbox[2])
                    & (pl.col("y") >= bbox[1])
                    & (pl.col("y") <= bbox[3])
                ).collect()
            _ = result
        elif query_type == "polygon":
            df = pl.scan_parquet(path)
            if is_s2:
                all_cids = [cid for cids in cell_ids_before_list for cid in cids]
                if all_cids:
                    _ = df.filter(pl.col("s2_cell").is_in(all_cids)).collect()
            elif is_h3:
                pass
            elif is_hilbert:
                all_cids = [cid for cids in cell_ids_before_list for cid in cids]
                if all_cids:
                    unique_cids = list(set(all_cids))
                    cid_df = pl.LazyFrame(pl.DataFrame({"z_index": unique_cids}, schema={"z_index": pl.UInt64}))
                    _ = df.join(cid_df, on="z_index", how="inner").collect()
            else:
                for poly_json in QUERY_POLYGONS:
                    pb = _polygon_bbox(poly_json)
                    _ = df.filter(
                        (pl.col("x") >= pb[0])
                        & (pl.col("x") <= pb[2])
                        & (pl.col("y") >= pb[1])
                        & (pl.col("y") <= pb[3])
                    ).collect()


def _query_geoparquet(tool_key: str, query_type: str, variant: str) -> None:
    """Execute a spatial query against the GeoParquet file."""
    path = str(get_path("geoparquet", variant))

    if tool_key == "duckdb":
        import duckdb
        con = duckdb.connect()
        con.execute("INSTALL spatial; LOAD spatial;")

        if query_type == "point":
            pts = QUERY_POINTS[:10]
            for lon, lat in pts:
                con.execute(
                    f"""SELECT band_value FROM read_parquet('{path}')
                        WHERE ST_Within(
                            geometry,
                            ST_Buffer(ST_MakePoint(?, ?), 0.000001)
                        )""",
                    [lon, lat],
                ).fetchall()
        elif query_type == "bbox":
            bbox = QUERY_BBOXES[0]
            con.execute(
                f"""SELECT band_value FROM read_parquet('{path}')
                    WHERE ST_Within(
                        geometry,
                        ST_MakeEnvelope(?, ?, ?, ?)
                    )""",
                [bbox[0], bbox[1], bbox[2], bbox[3]],
            ).fetchall()
        elif query_type == "polygon":
            for poly_json in QUERY_POLYGONS:
                wkt_poly = _geojson_to_wkt(poly_json)
                con.execute(
                    f"""SELECT band_value FROM read_parquet('{path}')
                        WHERE ST_Within(geometry, ST_GeomFromText(?))""",
                    [wkt_poly],
                ).fetchall()
        con.close()

def _query_lance(tool_key: str, query_type: str, variant: str) -> None:
    """Execute a spatial query against the Lance dataset using Hilbert index."""
    import lance
    path = str(get_path("lance", variant))
    dataset = lance.dataset(path)

    if query_type == "point":
        cell_ids_before = []
        pts = QUERY_POINTS[:10]
        for lon, lat in pts:
            cell_ids_before.extend(_hilbert_hierarchy_for_point(lon, lat))
    elif query_type == "bbox":
        cell_ids_before = _hilbert_cells_for_bbox(QUERY_BBOXES[0])
    elif query_type == "polygon":
        exterior_rings_tuples = []
        for poly_geojson in QUERY_POLYGONS:
            exterior_ring = poly_geojson["coordinates"][0]
            exterior_rings_tuples.append([tuple(coord) for coord in exterior_ring])
        import hilbert_plugin
        import polars_hilbert
        cell_ids_before = polars_hilbert.hilbert_cells_for_polygons(exterior_rings_tuples)
    else:
        cell_ids_before = []

    unique_cids = list(set(cell_ids_before))

    if tool_key == "duckdb":
        import duckdb
        con = duckdb.connect()
        if unique_cids:
            import pyarrow as pa
            arrow_table = pa.table({'z_index': pa.array(unique_cids, type=pa.uint64())})
            con.register('cids_table', arrow_table)
            query = f"SELECT COUNT(*) FROM lance_scan('{path}') INNER JOIN cids_table USING (z_index)"
            try:
                con.execute(query).fetchall()
            except Exception:
                lance_table = dataset.to_table()
                con.register('lance_table', lance_table)
                query = f"SELECT COUNT(*) FROM lance_table INNER JOIN cids_table USING (z_index)"
                con.execute(query).fetchall()
        con.close()
    elif tool_key == "lance_scanner":
        import pyarrow.compute as pc
        if unique_cids:
            # Native Lance filter using pyarrow compute
            _ = dataset.scanner(filter=pc.field("z_index").isin(unique_cids)).to_table()

def _query_zarr(tool_key: str, query_type: str, variant: str) -> None:
    """Execute a spatial query against the Zarr file."""
    zarr_path = str(get_path("zarr", variant))

    if tool_key == "xarray":
        import xarray as xr
        import rioxarray

        if query_type == "point":
            ds = xr.open_zarr(zarr_path)
            pts = QUERY_POINTS[:10]
            for lon, lat in pts:
                _ = ds.sel(x=lon, y=lat, method="nearest").compute()
            ds.close()
        elif query_type == "bbox":
            ds = xr.open_zarr(zarr_path)
            bbox = QUERY_BBOXES[0]
            _ = ds.sel(
                x=slice(bbox[0], bbox[2]),
                y=slice(bbox[3], bbox[1]),
            ).compute()
            ds.close()
        elif query_type == "polygon":
            ds = xr.open_zarr(zarr_path)
            ds.rio.write_crs("EPSG:4326", inplace=True)
            for poly_json in QUERY_POLYGONS:
                poly = shape(poly_json)
                clipped = ds.rio.clip([poly.__geo_interface__], ds.rio.crs).compute()
                _ = clipped
            ds.close()
    elif tool_key == "zarr_native":
        import zarr
        import rasterio
        import rasterio.features
        from rasterio.transform import from_bounds
        import numpy as np

        z = zarr.open(str(zarr_path), mode='r')
        if 'band_data' in z:
            data_array = z['band_data']
        elif 'band_value' in z:
            data_array = z['band_value']
        else:
            arrays = [v for k, v in z.arrays()]
            data_array = arrays[0]

        cog_path = FORMAT_PATHS["cog"][variant]
        with rasterio.open(cog_path) as src:
            transform = src.transform

        if query_type == "polygon":
            for poly_json in QUERY_POLYGONS:
                poly = shape(poly_json)
                pb = poly.bounds
                
                col_min, row_max = ~transform * (pb[0], pb[1])
                col_max, row_min = ~transform * (pb[2], pb[3])
                
                if col_min > col_max: col_min, col_max = col_max, col_min
                if row_min > row_max: row_min, row_max = row_max, row_min
                
                c_start = max(0, int(np.floor(col_min)))
                c_stop = min(data_array.shape[-1], int(np.ceil(col_max)))
                r_start = max(0, int(np.floor(row_min)))
                r_stop = min(data_array.shape[-2], int(np.ceil(row_max)))
                
                if r_start >= r_stop or c_start >= c_stop:
                    continue
                    
                if len(data_array.shape) == 2:
                    cropped_data = data_array[r_start:r_stop, c_start:c_stop]
                else:
                    cropped_data = data_array[0, r_start:r_stop, c_start:c_stop]
                    
                width = c_stop - c_start
                height = r_stop - r_start
                
                min_lon = transform.c + c_start * transform.a
                max_lat = transform.f + r_start * transform.e
                window_transform = rasterio.transform.Affine.translation(min_lon, max_lat) * rasterio.transform.Affine.scale(transform.a, transform.e)
                
                mask = rasterio.features.rasterize(
                    [(poly, 1)],
                    out_shape=(height, width),
                    transform=window_transform,
                    fill=0,
                    dtype=np.uint8
                )
                _ = mask
    elif tool_key == "zarrs_rust":
        import zarrs_plugin
        import rasterio
        cog_path = FORMAT_PATHS["cog"][variant]
        with rasterio.open(cog_path) as src:
            transform = src.transform
            t_tuple = (transform.a, transform.b, transform.c, transform.d, transform.e, transform.f)
            
        if query_type == "polygon":
            exterior_rings_tuples = []
            for polygon_geojson in QUERY_POLYGONS:
                exterior_ring = polygon_geojson["coordinates"][0]
                exterior_rings_tuples.append([tuple(coord) for coord in exterior_ring])
            _ = zarrs_plugin.zarrs_polygon_query(zarr_path, exterior_rings_tuples, t_tuple)


# ---------------------------------------------------------------------------
# Core benchmark runner (runs inside the child process)
# ---------------------------------------------------------------------------


def clear_cache():
    import os
    print("  [Clearing OS Page Cache... please enter password if prompted]", flush=True)
    os.system("sudo purge")

def _execute_benchmark(format_key: str, tool_key: str, query_type: str, data_variant: str) -> dict:
    """Dispatch to the correct query function, measure wall-clock time and RSS.

    RSS is captured as a single post-query snapshot (not a peak), stored as
    ``final_rss_mb``.
    """
    process = psutil.Process()
    t0 = time.perf_counter()

    if format_key == "cog":
        _query_cog(tool_key, query_type, data_variant)
    elif format_key == "zarr":
        _query_zarr(tool_key, query_type, data_variant)
    elif format_key in ("parquet_flat", "parquet_s2", "parquet_h3", "parquet_hilbert"):
        _query_parquet(format_key, tool_key, query_type, data_variant)
    elif format_key == "geoparquet":
        _query_geoparquet(tool_key, query_type, data_variant)
    elif format_key == "lance":
        _query_lance(tool_key, query_type, data_variant)
    else:
        raise ValueError(f"Unknown format_key: {format_key}")

    duration_ms = (time.perf_counter() - t0) * 1000
    final_rss_mb = process.memory_info().rss / (1024 * 1024)

    return {
        "format": format_key,
        "format_label": FORMAT_LABELS.get(format_key, format_key),
        "tool": tool_key,
        "tool_label": TOOL_LABELS.get(tool_key, tool_key),
        "query_type": query_type,
        "data_variant": data_variant,
        "data_variant_label": VARIANT_LABELS.get(data_variant, data_variant),
        "duration_ms": round(duration_ms, 2),
        "final_rss_mb": round(final_rss_mb, 2),
        "filesize_mb": _filesize_mb(format_key, data_variant),
    }


# ---------------------------------------------------------------------------
# Subprocess worker (invoked via multiprocessing.Process)
# ---------------------------------------------------------------------------


def _benchmark_worker(
    queue: multiprocessing.Queue,
    format_key: str,
    tool_key: str,
    query_type: str,
    data_variant: str,
    report_path: str,
) -> None:
    """Entry-point for the child process.  Wraps the query in memray.Tracker."""
    import memray
    try:
        with memray.Tracker(report_path):
            result = _execute_benchmark(format_key, tool_key, query_type, data_variant)
        queue.put({"status": "success", **result})
    except Exception as exc:
        queue.put({
            "status": "crash",
            "format": format_key,
            "format_label": FORMAT_LABELS.get(format_key, format_key),
            "tool": tool_key,
            "tool_label": TOOL_LABELS.get(tool_key, tool_key),
            "query_type": query_type,
            "data_variant": data_variant,
            "data_variant_label": VARIANT_LABELS.get(data_variant, data_variant),
            "error": f"{type(exc).__name__}: {exc}",
            "traceback": traceback.format_exc(),
        })


# ---------------------------------------------------------------------------
# Timeout-protected runner (called from the parent process)
# ---------------------------------------------------------------------------


def _run_with_memray(
    format_key: str, tool_key: str, query_type: str, run_index: int, data_variant: str,
) -> dict:
    """Spawn a subprocess that benchmarks *format_key / tool_key / query_type*.

    The subprocess is killed if it exceeds *TIMEOUT_SECONDS*.  Returns a
    result dict that always includes a ``status`` key.
    """
    mem_dir = Path("results/memray")
    mem_dir.mkdir(parents=True, exist_ok=True)
    
    # Shorten names to avoid macOS 104-char UNIX socket limit
    short_fmt = format_key.replace("parquet", "pq").replace("_flat", "f").replace("_s2", "s2").replace("_h3", "h3").replace("geoparquet", "gpq")
    report_name = f"{short_fmt}_{tool_key[:3]}_{query_type[:3]}_{data_variant}_{run_index:02d}.bin"
    report_path = mem_dir / report_name

    if report_path.exists():
        report_path.unlink()

    ctx = multiprocessing.get_context("spawn")
    queue: multiprocessing.Queue = ctx.Queue()
    proc = ctx.Process(
        target=_benchmark_worker,
        args=(queue, format_key, tool_key, query_type, data_variant, str(report_path)),
    )
    proc.start()
    proc.join(timeout=TIMEOUT_SECONDS)

    if proc.is_alive():
        proc.kill()
        proc.join(timeout=5)
        return {
            "status": "timeout",
            "format": format_key,
            "format_label": FORMAT_LABELS.get(format_key, format_key),
            "tool": tool_key,
            "tool_label": TOOL_LABELS.get(tool_key, tool_key),
            "query_type": query_type,
            "data_variant": data_variant,
            "data_variant_label": VARIANT_LABELS.get(data_variant, data_variant),
            "duration_ms": None,
            "final_rss_mb": None,
            "filesize_mb": _filesize_mb(format_key, data_variant),
            "run": run_index,
        }

    try:
        result = queue.get_nowait()
    except Exception:
        return {
            "status": "crash",
            "format": format_key,
            "format_label": FORMAT_LABELS.get(format_key, format_key),
            "tool": tool_key,
            "tool_label": TOOL_LABELS.get(tool_key, tool_key),
            "query_type": query_type,
            "data_variant": data_variant,
            "data_variant_label": VARIANT_LABELS.get(data_variant, data_variant),
            "duration_ms": None,
            "final_rss_mb": None,
            "filesize_mb": _filesize_mb(format_key, data_variant),
            "run": run_index,
            "error": "Subprocess died without writing to queue",
        }

    result["run"] = run_index
    return result


# ---------------------------------------------------------------------------
# Memray peak extraction (post-hoc, for optional enrichment)
# ---------------------------------------------------------------------------


def _read_memray_peak(report_path: str) -> float | None:
    """Extract the peak memory (MiB) recorded by a memray report.

    Uses ``python -m memray stats`` CLI since the Python FileReader API
    for peak extraction is verbose.  Returns *None* if the report is missing
    or parsing fails.
    """
    rp = Path(report_path)
    if not rp.exists():
        return None
    try:
        proc = subprocess.run(
            [sys.executable, "-m", "memray", "stats", str(rp)],
            capture_output=True, text=True, timeout=30,
        )
        for line in proc.stdout.splitlines():
            # memray >=1.15 emits a line like "Peak memory: 123.45 MB"
            if "Peak memory" in line or "peak memory" in line:
                # Pull out the numeric part
                parts = line.replace(",", "").split()
                for p in parts:
                    stripped = p.rstrip("MBmb")
                    try:
                        return float(stripped)
                    except ValueError:
                        continue
        return None
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


def main() -> None:
    print("=== Stage 2: Benchmarking ===")
    t_total = time.time()

    available_tools = check_imports()
    print(f"Available tools: {', '.join(sorted(available_tools))}")

    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    MEMRAY_DIR.mkdir(parents=True, exist_ok=True)

    all_rows: list[dict] = []
    existing_runs = set()
    results_path = RESULTS_DIR / "benchmarks.parquet"
    if results_path.exists():
        try:
            existing_df = pd.read_parquet(results_path)
            all_rows = existing_df.to_dict(orient="records")
            for row in all_rows:
                # We skip re-running if status is success, or even if crash/timeout,
                # to avoid getting stuck retrying failing tests, unless you want to retry crashes.
                # Let's skip if it's already in the file.
                existing_runs.add((row["format"], row["tool"], row["query_type"], row["data_variant"], int(row["run"])))
            print(f"Loaded {len(all_rows)} existing benchmark runs. Skipping these...")
        except Exception as e:
            print(f"Failed to load existing benchmarks: {e}")

    # Count total combos for progress tracking
    combo_runs: list[tuple] = []
    for format_key, tool_key, query_type in BENCHMARK_COMBOS:
        if tool_key not in available_tools:
            continue
        for variant in DATA_VARIANTS:
            data_path = get_path(format_key, variant)
            if data_path.exists():
                combo_runs.append((format_key, tool_key, query_type, variant))
    total_combos = len(combo_runs) * NUM_RUNS
    print(f"Total benchmark runs: {total_combos} ({len(combo_runs)} combos x {NUM_RUNS} runs)")

    completed = 0
    for format_key, tool_key, query_type, variant in combo_runs:
        data_path = get_path(format_key, variant)
        label = f"{format_key} [{variant}] | {tool_key} | {query_type}"

        skip_remaining = False
        for run_idx in range(NUM_RUNS):
            completed += 1
            
            run_key = (format_key, tool_key, query_type, variant, run_idx)
            if run_key in existing_runs:
                # We don't print anything to avoid spamming the console
                continue

            if skip_remaining:
                print(f"[{completed}/{total_combos}] {label}  run {run_idx + 1}/{NUM_RUNS} ... SKIP (early termination)")
                all_rows.append({
                    "status": "skipped",
                    "format": format_key,
                    "format_label": FORMAT_LABELS.get(format_key, format_key),
                    "tool": tool_key,
                    "tool_label": TOOL_LABELS.get(tool_key, tool_key),
                    "query_type": query_type,
                    "data_variant": variant,
                    "data_variant_label": VARIANT_LABELS.get(variant, variant),
                    "duration_ms": None,
                    "final_rss_mb": None,
                    "filesize_mb": _filesize_mb(format_key, variant),
                    "run": run_idx,
                })
                continue

            print(f"[{completed}/{total_combos}] {label}  run {run_idx + 1}/{NUM_RUNS} ...", end=" ", flush=True)

            t_run = time.time()
            row = _run_with_memray(format_key, tool_key, query_type, run_idx, variant)
            elapsed_run = time.time() - t_run

            if row["status"] == "success":
                print(f"OK  {row['duration_ms']:.0f} ms  {row['final_rss_mb']:.0f} MiB")
            elif row["status"] == "timeout":
                print(f"TIMEOUT (>{TIMEOUT_SECONDS}s)")
                if run_idx == 0:
                    skip_remaining = True
                    print(f"  -> First run timed out, skipping remaining {NUM_RUNS - 1} runs for this combo")
            else:
                err_detail = row.get("error", "unknown")
                print(f"CRASH  ({err_detail[:80]})")

            # Optional: enrich with memray peak
            report_path = str(
                MEMRAY_DIR / f"{format_key}__{tool_key}__{query_type}__{variant}__run{run_idx:02d}.bin"
            )
            memray_peak = _read_memray_peak(report_path)
            if memray_peak is not None:
                row["memray_peak_mb"] = round(memray_peak, 2)

            row["wall_seconds"] = round(elapsed_run, 2)
            all_rows.append(row)

    # ------------------------------------------------------------------
    # Save results
    # ------------------------------------------------------------------
    df = pd.DataFrame(all_rows)
    output_path = RESULTS_DIR / "benchmarks.parquet"
    df.to_parquet(output_path, index=False)
    print(f"\nSaved {len(df)} rows to {output_path}")

    # Quick summary
    success_df = df[df["status"] == "success"]
    if len(success_df) > 0:
        print(f"\nSuccessful runs: {len(success_df)}")
        print(f"  Median duration: {success_df['duration_ms'].median():.1f} ms")
        print(f"  Median final RSS: {success_df['final_rss_mb'].median():.1f} MiB")

    crash_count = len(df[df["status"] == "crash"])
    timeout_count = len(df[df["status"] == "timeout"])
    skipped_count = len(df[df["status"] == "skipped"])
    if crash_count:
        print(f"  Crashes: {crash_count}")
    if timeout_count:
        print(f"  Timeouts: {timeout_count}")
    if skipped_count:
        print(f"  Skipped (early termination): {skipped_count}")

    # Per-variant summary
    for variant in DATA_VARIANTS:
        vdf = success_df[success_df["data_variant"] == variant] if len(success_df) > 0 else success_df
        if len(vdf) > 0:
            print(f"  [{variant}] {len(vdf)} successes, median {vdf['duration_ms'].median():.1f} ms")

    elapsed_total = time.time() - t_total
    print(f"\nBenchmarking complete in {elapsed_total:.0f}s")


if __name__ == "__main__":
    main()
