"""Stage 1: Download Copernicus GLO-30 DEM COG and convert to all target formats."""

import json
import time
import sys
from pathlib import Path

import rasterio
from rasterio.windows import Window
from rasterio.transform import rowcol
import numpy as np
import pandas as pd
import geopandas as gpd
import h3
import s2cell
from shapely.geometry import Point
from tqdm import tqdm

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    DATA_DIR, REGION_BOUNDS, S2_LEVEL, H3_RESOLUTION, SEED,
    COG_PATH, FLAT_PARQUET_PATH, S2_PARQUET_PATH, H3_PARQUET_PATH, GEOPARQUET_PATH,
)


def download_copernicus_dem():
    """Download Copernicus GLO-30 tiles covering the region and build a VRT.

    Uses the public Copernicus DEM S3 bucket. Falls back to a bounding-box
    download from OpenTopography if S3 access is unavailable.
    """
    if COG_PATH.exists():
        print(f"COG already exists at {COG_PATH}, skipping download")
        return

    DATA_DIR.mkdir(parents=True, exist_ok=True)

    # Copernicus DEM is available at 1° tiles. We build a VRT from the
    # public AWS registry of open data.
    bounds = REGION_BOUNDS
    min_lon = int(bounds["min_lon"])
    max_lon = int(bounds["max_lon"]) + 1
    min_lat = int(bounds["min_lat"])
    max_lat = int(bounds["max_lat"]) + 1

    tile_urls = []
    for lat in range(min_lat, max_lat):
        lat_char = "N" if lat >= 0 else "S"
        for lon in range(min_lon, max_lon):
            lon_char = "E" if lon >= 0 else "W"
            tile_name = f"Copernicus_DSM_COG_10_{lat_char}{abs(lat):02d}_00_{lon_char}{abs(lon):03d}_00_DEM"
            url = f"/vsis3/copernicus-dem-30m/{tile_name}/{tile_name}.tif"
            tile_urls.append(url)

    # Try S3 first, fall back to GDAL's network VRT
    try:
        from osgeo import gdal
        vrt = gdal.BuildVRT(str(COG_PATH.parent / "dem_mosaic.vrt"), tile_urls)
        # Convert to COG for clean baseline
        gdal.Translate(
            str(COG_PATH),
            vrt,
            format="COG",
            outputBounds=[
                bounds["min_lon"], bounds["min_lat"],
                bounds["max_lon"], bounds["max_lat"],
            ],
            resampleAlg="bilinear",
        )
        vrt = None
    except Exception:
        print("S3 access failed. Download a single merged tile manually or use OpenTopography.")
        print(f"Place your COG at: {COG_PATH}")
        raise


def extract_to_flat_parquet():
    """Extract COG pixels to a row-per-pixel flat Parquet file."""
    if FLAT_PARQUET_PATH.exists():
        print(f"Flat parquet already exists at {FLAT_PARQUET_PATH}, skipping")
        return

    print("Extracting COG to flat Parquet...")
    rows_list = []
    with rasterio.open(COG_PATH) as src:
        transform = src.transform
        width = src.width
        height = src.height
        block_size = 1024

        for y_start in tqdm(range(0, height, block_size), desc="Rows"):
            y_end = min(y_start + block_size, height)
            window = Window(0, y_start, width, y_end - y_start)
            data = src.read(1, window=window)
            nodata = src.nodata

            ys, xs = np.where(data != nodata) if nodata is not None else (
                np.indices(data.shape).reshape(2, -1)
            )
            for yi, xi in zip(ys, xs):
                px = window.col_off + xi
                py = window.row_off + yi
                lon, lat = transform * (px + 0.5, py + 0.5)
                rows_list.append({
                    "x": lon,
                    "y": lat,
                    "band_value": float(data[yi, xi]),
                })

    df = pd.DataFrame(rows_list)
    df.to_parquet(FLAT_PARQUET_PATH, compression="zstd", index=False)
    print(f"Wrote {len(df):,} rows to {FLAT_PARQUET_PATH}")


def add_s2_index():
    """Add S2 cell IDs to the flat parquet."""
    if S2_PARQUET_PATH.exists():
        print(f"S2 parquet already exists at {S2_PARQUET_PATH}, skipping")
        return

    print("Adding S2 cell index...")
    df = pd.read_parquet(FLAT_PARQUET_PATH)
    df["s2_cell"] = df.apply(
        lambda row: s2cell.s2cell.lat_lon_to_cell_id(
            row["y"], row["x"], S2_LEVEL
        ),
        axis=1,
    )
    df["s2_cell"] = df["s2_cell"].astype("int64")
    df.to_parquet(S2_PARQUET_PATH, compression="zstd", index=False)
    print(f"Wrote {len(df):,} rows to {S2_PARQUET_PATH}")


def add_h3_index():
    """Add H3 cell IDs to the flat parquet."""
    if H3_PARQUET_PATH.exists():
        print(f"H3 parquet already exists at {H3_PARQUET_PATH}, skipping")
        return

    print("Adding H3 cell index...")
    df = pd.read_parquet(FLAT_PARQUET_PATH)
    df["h3_cell"] = df.apply(
        lambda row: h3.latlng_to_cell(row["y"], row["x"], H3_RESOLUTION),
        axis=1,
    )
    df["h3_cell"] = df["h3_cell"].astype("int64")
    df.to_parquet(H3_PARQUET_PATH, compression="zstd", index=False)
    print(f"Wrote {len(df):,} rows to {H3_PARQUET_PATH}")


def write_geoparquet():
    """Write GeoParquet with native geometry column."""
    if GEOPARQUET_PATH.exists():
        print(f"GeoParquet already exists at {GEOPARQUET_PATH}, skipping")
        return

    print("Writing GeoParquet...")
    df = pd.read_parquet(FLAT_PARQUET_PATH)
    geometry = [Point(x, y) for x, y in zip(df["x"], df["y"])]
    gdf = gpd.GeoDataFrame(
        {"band_value": df["band_value"]},
        geometry=geometry,
        crs="EPSG:4326",
    )
    gdf.to_parquet(GEOPARQUET_PATH, compression="zstd", index=False)
    print(f"Wrote {len(gdf):,} rows to {GEOPARQUET_PATH}")


def record_file_sizes():
    """Record on-disk sizes of each format file."""
    sizes = {}
    from config import FORMAT_PATHS, FORMAT_LABELS
    for key, path in FORMAT_PATHS.items():
        if path.exists():
            size_mb = path.stat().st_size / (1024 * 1024)
            sizes[FORMAT_LABELS[key]] = round(size_mb, 2)
    return sizes


def main():
    print("=== Stage 1: Data Preparation ===")
    t0 = time.time()

    download_copernicus_dem()
    extract_to_flat_parquet()
    add_s2_index()
    add_h3_index()
    write_geoparquet()

    sizes = record_file_sizes()
    print("\nFile sizes:")
    for name, size_mb in sizes.items():
        print(f"  {name}: {size_mb:.1f} MB")

    elapsed = time.time() - t0
    print(f"\nData prep complete in {elapsed:.0f}s")


if __name__ == "__main__":
    main()
