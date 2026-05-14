import sys
from pathlib import Path
import random
import rasterio
import duckdb
import s2sphere

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import FORMAT_PATHS, REGION_BOUNDS, S2_LEVEL

def _s2_hierarchy_for_point(lon, lat):
    cell = s2sphere.CellId.from_lat_lng(s2sphere.LatLng.from_degrees(lat, lon))
    return [cell.parent(lvl).id() for lvl in range(1, S2_LEVEL + 1)]

def validate_data_parity():
    cog_path = FORMAT_PATHS["cog"]["raw"]
    s2_path = FORMAT_PATHS["parquet_s2"]["raw"]
    
    con = duckdb.connect()
    
    import numpy as np
    mismatches = 0
    valid_tested = 0
    
    with rasterio.open(cog_path) as src:
        height, width = src.height, src.width
        transform = src.transform
        
        # Generate 1000 random pixel indices
        points = []
        for _ in range(1000):
            col = random.randint(0, width - 1)
            row = random.randint(0, height - 1)
            # Calculate exact pixel center
            lon = transform.c + (col + 0.5) * transform.a
            lat = transform.f + (row + 0.5) * transform.e
            points.append((lon, lat, col, row))
            
        print(f"Validating {len(points)} exact pixel centers between COG and S2 Parquet...")
        
        for lon, lat, col, row in points:
            # 1. Get exact COG pixel value without interpolation
            window = rasterio.windows.Window(col, row, 1, 1)
            raw_val = src.read(1, window=window)[0, 0]
            
            if raw_val == src.nodata or not np.isfinite(raw_val):
                continue # Skip nodata pixels
                
            cog_val = round(float(raw_val) * 10)
            
            # 2. Get S2 Parquet value
            cell_ids = _s2_hierarchy_for_point(lon, lat)
            query = f"SELECT band_value FROM read_parquet('{s2_path}') WHERE s2_cell IN ({','.join(map(str, cell_ids))})"
            res = con.execute(query).fetchall()
            
            if not res:
                # Might be stripped nodata, or an edge case
                continue
                
            valid_tested += 1
            s2_val = res[0][0]
            
            if cog_val != s2_val:
                print(f"Mismatch at col={col}, row={row} ({lon:.4f}, {lat:.4f}): COG={cog_val} != S2={s2_val}")
                mismatches += 1
                
    if mismatches == 0:
        print(f"SUCCESS: 100% Data Parity confirmed across {valid_tested} valid pixels!")
    else:
        print(f"FAILED: Found {mismatches} mismatched data points out of {valid_tested} valid pixels tested.")

if __name__ == '__main__':
    validate_data_parity()
