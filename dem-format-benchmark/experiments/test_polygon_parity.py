import sys
from pathlib import Path
import rasterio
import duckdb
import numpy as np
from shapely.geometry import shape

sys.path.insert(0, str(Path('./scripts').resolve().parent))
from config import FORMAT_PATHS, QUERY_POLYGONS
from scripts.zorder_plugin import zorder_cells_for_polygon

cog_path = FORMAT_PATHS['cog']['raw']
zorder_path = FORMAT_PATHS['parquet_zorder']['raw']
con = duckdb.connect()

poly_geojson = QUERY_POLYGONS[0]
poly = shape(poly_geojson)

# 1. Rasterio masking
import rasterio.mask
with rasterio.open(cog_path) as src:
    masked_data, _ = rasterio.mask.mask(src, [poly], crop=True)
    # Count valid pixels
    valid_mask = np.isfinite(masked_data)
    if src.nodata is not None and np.isfinite(src.nodata):
        valid_mask &= (masked_data != src.nodata)
    rast_count = np.count_nonzero(valid_mask)
    rast_sum = np.sum(np.round(masked_data[valid_mask] * 10).astype(np.int16))

# 2. Z-order parquet
cids = zorder_cells_for_polygon(poly_geojson)
query = f"SELECT band_value FROM read_parquet('{zorder_path}') WHERE z_index IN ({','.join(map(str, cids))})"
res = con.execute(query).fetchall()

# The result might contain some pixels that are just outside or inside depending on the exact Point-in-Polygon vs raster boundary logic,
# but they should be very close. Let's see the counts and sums.
pq_count = len(res)
pq_sum = sum([r[0] for r in res])

print(f"Rasterio: count={rast_count}, sum={rast_sum}")
print(f"Z-order Parquet: count={pq_count}, sum={pq_sum}")
