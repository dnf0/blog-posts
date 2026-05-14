use std::sync::Arc;
use object_store::aws::AmazonS3Builder;
use zarrs_object_store::AsyncObjectStore;
use zarrs::array::{Array, ArraySubset};
use tokio::time::Instant;
use std::fs;
use serde::Deserialize;
use futures::future::join_all;
use geo_rasterize::{BinaryRasterizer, Transform};
use geo_types::{Polygon, LineString, coord};
use euclid::Transform2D;

#[derive(Deserialize)]
struct PolygonFeature {
    coordinates: Vec<Vec<Vec<f64>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("=== Pure Rust S3 Benchmark ===");

    // Read transform
    let transform_str = fs::read_to_string("transform.json")?;
    let tr: Vec<f64> = serde_json::from_str(&transform_str)?;
    let (a, b, c, d, e, f) = (tr[0], tr[1], tr[2], tr[3], tr[4], tr[5]);

    // Read polygons
    let polys_str = fs::read_to_string("polys.json")?;
    let polys: Vec<PolygonFeature> = serde_json::from_str(&polys_str)?;

    let t0 = Instant::now();
    let s3 = AmazonS3Builder::from_env()
        .with_region("eu-west-2")
        .with_bucket_name("chunked-rasters")
        .build()?;
    let prefix = object_store::path::Path::parse("benchmark_test/dem_q2500.zarr/__xarray_dataarray_variable__")?;
    let prefixed_s3 = object_store::prefix::PrefixStore::new(s3, prefix);
    let store = Arc::new(AsyncObjectStore::new(prefixed_s3));
    let array = Array::async_open(store.clone(), "/").await?;
    let array_shape = array.shape().to_vec();
    
    let meta_time = t0.elapsed();
    println!("  -> Zarr Metadata read: {:.3} s", meta_time.as_secs_f64());

    let t1 = Instant::now();
    let mut tasks = Vec::new();
    
    // To ensure thread safety, we wrap the array in an Arc.
    let array = Arc::new(array);

    for p in polys {
        let exterior_ring = p.coordinates[0].clone();
        let array = array.clone();
        let array_shape = array_shape.clone();
        
        tasks.push(tokio::spawn(async move {
            let mut min_lon = f64::MAX;
            let mut max_lon = f64::MIN;
            let mut min_lat = f64::MAX;
            let mut max_lat = f64::MIN;
            
            let mut coords = Vec::with_capacity(exterior_ring.len());
            for pt in exterior_ring {
                let lon = pt[0];
                let lat = pt[1];
                min_lon = min_lon.min(lon);
                max_lon = max_lon.max(lon);
                min_lat = min_lat.min(lat);
                max_lat = max_lat.max(lat);
                coords.push(coord! { x: lon, y: lat });
            }
            let poly = Polygon::new(LineString::new(coords), vec![]);
            
            let col_1 = (min_lon - c) / a;
            let col_2 = (max_lon - c) / a;
            let row_1 = (min_lat - f) / e;
            let row_2 = (max_lat - f) / e;
            
            let col_min = col_1.min(col_2).floor() as u64;
            let col_max = col_1.max(col_2).ceil() as u64;
            let row_min = row_1.min(row_2).floor() as u64;
            let row_max = row_1.max(row_2).ceil() as u64;
            
            let c_start = col_min.max(0);
            let c_stop = col_max.min(array_shape[2]);
            let r_start = row_min.max(0);
            let r_stop = row_max.min(array_shape[1]);
            
            if r_start >= r_stop || c_start >= c_stop {
                return 0u64;
            }

            let subset = ArraySubset::new_with_ranges(&[0..1, r_start..r_stop, c_start..c_stop]);
            
            // Retrieve bytes concurrently over S3
            if let Ok(data) = array.async_retrieve_array_subset::<Vec<i16>>(&subset).await {
                // To do the exact rasterize math:
                let width = (c_stop - c_start) as usize;
                let height = (r_stop - r_start) as usize;
                let origin_lon = c + (c_start as f64) * a;
                let origin_lat = f + (r_start as f64) * e;
                let pix_to_geo: Transform = Transform2D::new(
                    a, d,
                    b, e,
                    origin_lon, origin_lat
                );
                let geo_to_pix = pix_to_geo.inverse().unwrap();
                let mut rasterizer = BinaryRasterizer::new(width, height, Some(geo_to_pix)).unwrap();
                rasterizer.rasterize(&poly).unwrap();
                let arr = rasterizer.finish();
                
                let mut count = 0;
                for (i, &val) in arr.iter().enumerate() {
                    if val && data[i] != -32768 {
                        count += 1;
                    }
                }
                count
            } else {
                0
            }
        }));
    }

    let results = join_all(tasks).await;
    let total: u64 = results.into_iter().map(|r| r.unwrap_or(0)).sum();
    
    let exec_time = t1.elapsed();
    let total_time = t0.elapsed();
    
    println!("  -> Total pixels extracted: {}", total);
    println!("  -> Extraction time (1000 polys): {:.3} s", exec_time.as_secs_f64());
    println!("Zarr (Pure Rust S3): {:.2} s", total_time.as_secs_f64());

    Ok(())
}