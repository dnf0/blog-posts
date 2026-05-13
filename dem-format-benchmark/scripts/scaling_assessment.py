import time
import rasterio
import rasterio.mask
import polars as pl
import duckdb
import numpy as np
from shapely.geometry import shape
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import FORMAT_PATHS, generate_query_polygons
from scripts.hilbert_plugin import hilbert_cells_for_polygons

def benchmark_rasterio(cog_path, polygons):
    import concurrent.futures
    import threading
    t0 = time.time()
    
    # We use ThreadPoolExecutor because rasterio releases the GIL during the C++ read/masking.
    # To be thread-safe with rasterio, we open a separate handle per thread, or open one inside the task.
    # Opening inside the task is safer but adds overhead.
    # Alternatively, use a thread-local store.
    
    thread_local = threading.local()

    def get_src():
        if not hasattr(thread_local, "src"):
            thread_local.src = rasterio.open(cog_path)
        return thread_local.src
        
    def process_poly(poly_geojson):
        src = get_src()
        poly = shape(poly_geojson)
        masked_data, _ = rasterio.mask.mask(src, [poly], crop=True)
        valid_mask = np.isfinite(masked_data)
        if src.nodata is not None and np.isfinite(src.nodata):
            valid_mask &= (masked_data != src.nodata)
        return np.count_nonzero(valid_mask)

    with concurrent.futures.ThreadPoolExecutor() as executor:
        results = list(executor.map(process_poly, polygons))
        
    count = sum(results)
    return time.time() - t0, count

def benchmark_polars(parquet_path, polygons):
    t0 = time.time()
    
    # 1. Precompute cells (Client-side region covering)
    t_cover = time.time()
    unique_cids = hilbert_cells_for_polygons(polygons)
    cover_time = time.time() - t_cover
    
    # 2. Query execution
    t_query = time.time()
    df = pl.scan_parquet(parquet_path)
    if unique_cids:
        cid_df = pl.LazyFrame(pl.DataFrame({"z_index": unique_cids}, schema={"z_index": pl.UInt64}))
        res = df.join(cid_df, on="z_index", how="inner").collect()
        count = len(res)
    else:
        count = 0
    query_time = time.time() - t_query
    
    total_time = time.time() - t0
    return total_time, cover_time, query_time, count

def benchmark_lance(lance_path, polygons):
    import lance
    import duckdb
    dataset = lance.dataset(lance_path)
    
    t0 = time.time()
    
    # 1. Precompute cells (Client-side region covering)
    t_cover = time.time()
    unique_cids = hilbert_cells_for_polygons(polygons)
    cover_time = time.time() - t_cover

    # 2. Query execution
    t_query = time.time()
    con = duckdb.connect()
    count = 0
    if unique_cids:
        import pyarrow as pa
        arrow_table = pa.table({'z_index': pa.array(unique_cids, type=pa.uint64())})
        con.register('cids_table', arrow_table)
        try:
            query = f"SELECT COUNT(*) FROM lance_scan('{lance_path}') INNER JOIN cids_table USING (z_index)"
            res = con.execute(query).fetchone()
            count = res[0]
        except Exception:
            lance_table = dataset.to_table()
            con.register('lance_table', lance_table)
            query = f"SELECT COUNT(*) FROM lance_table INNER JOIN cids_table USING (z_index)"
            res = con.execute(query).fetchone()
            count = res[0]
    con.close()
    query_time = time.time() - t_query
    
    total_time = time.time() - t0
    return total_time, cover_time, query_time, count

def benchmark_duckdb(parquet_path, polygons):
    t0 = time.time()
    
    # 1. Precompute cells (Client-side region covering)
    t_cover = time.time()
    unique_cids = hilbert_cells_for_polygons(polygons)
    cover_time = time.time() - t_cover

    # 2. Query execution
    t_query = time.time()
    con = duckdb.connect()
    count = 0
    if unique_cids:
        import pyarrow as pa
        arrow_table = pa.table({'z_index': pa.array(unique_cids, type=pa.uint64())})
        con.register('cids_table', arrow_table)
        query = f"SELECT COUNT(*) FROM read_parquet('{parquet_path}') INNER JOIN cids_table USING (z_index)"
        res = con.execute(query).fetchone()
        count = res[0]
    con.close()
    query_time = time.time() - t_query
    
    total_time = time.time() - t0
    return total_time, cover_time, query_time, count

def benchmark_zarr_rust(zarr_path, cog_path, polygons):
    import zarrs_plugin
    t0 = time.time()
    
    with rasterio.open(cog_path) as src:
        transform = src.transform
        t_tuple = (transform.a, transform.b, transform.c, transform.d, transform.e, transform.f)

    exterior_rings_tuples = []
    for polygon_geojson in polygons:
        exterior_ring = polygon_geojson["coordinates"][0]
        exterior_rings_tuples.append([tuple(coord) for coord in exterior_ring])
        
    count = zarrs_plugin.zarrs_polygon_query(zarr_path, exterior_rings_tuples, t_tuple)
    
    total_time = time.time() - t0
    return total_time, count

def clear_cache():
    import os
    print("  [Clearing OS Page Cache... please enter password if prompted]")
    os.system("sudo purge")

def main():
    cog_path = str(FORMAT_PATHS["cog"]["raw"])
    # We use q2500 for parquet as it demonstrates the ideal compressed structure.
    hilbert_path = str(FORMAT_PATHS["parquet_hilbert"]["q2500"])
    zarr_path = str(FORMAT_PATHS["zarr"]["q2500"])
    lance_path = str(FORMAT_PATHS["lance"]["q2500"])
    
    batch_sizes = [10, 100, 1000]
    
    print("=== Scaling Assessment: Polygons ===")
    print(f"Raster: {cog_path}")
    print(f"Parquet: {hilbert_path}")
    print(f"Zarr: {zarr_path}")
    print(f"Lance: {lance_path}\n")
    
    results = []

    for n in batch_sizes:
        print(f"--- Batch size: {n} Polygons ---")
        polygons = generate_query_polygons(n=n, seed=42)
        
        # 1. Rasterio
        try:
            r_time, r_count = benchmark_rasterio(cog_path, polygons)
            print(f"Rasterio + COG:       {r_time:7.3f} s (Count: {r_count})")
            results.append({"tool": "Rasterio + COG", "batch_size": n, "total_time_s": r_time, "query_time_s": r_time, "cover_time_s": 0.0})
        except Exception as e:
            print(f"Rasterio failed: {e}")
            
        # 2. Polars
        try:
            p_tot, p_cov, p_q, p_count = benchmark_polars(hilbert_path, polygons)
            print(f"Polars + Parquet:     {p_tot:7.3f} s (Cover: {p_cov:5.3f}s, Query: {p_q:5.3f}s) (Count approx: {p_count})")
            results.append({"tool": "Polars + Hilbert Parquet", "batch_size": n, "total_time_s": p_tot, "query_time_s": p_q, "cover_time_s": p_cov})
        except Exception as e:
            print(f"Polars failed: {e}")

        # 3. DuckDB
        try:
            d_tot, d_cov, d_q, d_count = benchmark_duckdb(hilbert_path, polygons)
            print(f"DuckDB + Parquet:     {d_tot:7.3f} s (Cover: {d_cov:5.3f}s, Query: {d_q:5.3f}s) (Count approx: {d_count})")
            results.append({"tool": "DuckDB + Hilbert Parquet", "batch_size": n, "total_time_s": d_tot, "query_time_s": d_q, "cover_time_s": d_cov})
        except Exception as e:
            print(f"DuckDB failed: {e}")
            
        # 4. Zarrs Rust
        try:
            zr_tot, zr_count = benchmark_zarr_rust(zarr_path, cog_path, polygons)
            print(f"Pure Rust Zarr:       {zr_tot:7.3f} s (Count: {zr_count})")
            results.append({"tool": "Pure Rust Zarr", "batch_size": n, "total_time_s": zr_tot, "query_time_s": zr_tot, "cover_time_s": 0.0})
        except Exception as e:
            print(f"Zarrs Rust failed: {e}")

        # 5. Lance
        try:
            l_tot, l_cov, l_q, l_count = benchmark_lance(lance_path, polygons)
            print(f"DuckDB + Lance:       {l_tot:7.3f} s (Cover: {l_cov:5.3f}s, Query: {l_q:5.3f}s) (Count approx: {l_count})")
            results.append({"tool": "DuckDB + Lance", "batch_size": n, "total_time_s": l_tot, "query_time_s": l_q, "cover_time_s": l_cov})
        except Exception as e:
            print(f"Lance failed: {e}")
        
        print()

    import pandas as pd
    df = pd.DataFrame(results)
    out_path = Path("results/scaling.parquet")
    out_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(out_path)
    print(f"Saved scaling results to {out_path}")

if __name__ == '__main__':
    main()
