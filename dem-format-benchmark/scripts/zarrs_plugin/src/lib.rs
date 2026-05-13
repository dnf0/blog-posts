use pyo3::prelude::*;
use zarrs::{
    array::{Array, ArraySubset},
    filesystem::FilesystemStore,
};
use std::sync::Arc;
use geo_rasterize::{BinaryRasterizer, Transform};
use geo_types::{Polygon, LineString, coord};
use euclid::Transform2D;
use rayon::prelude::*;

#[pyfunction]
fn zarrs_polygon_query(store_path: &str, polygons: Vec<Vec<(f64, f64)>>, transform: (f64, f64, f64, f64, f64, f64)) -> PyResult<u64> {
    let store = Arc::new(FilesystemStore::new(store_path).unwrap());
    
    // Open the array
    let array = Array::open(store, "/__xarray_dataarray_variable__").unwrap();
    let array_shape = array.shape();
    
    let a = transform.0;
    let b = transform.1;
    let c = transform.2;
    let d = transform.3;
    let e = transform.4;
    let f = transform.5;

    let total_count: u64 = polygons.into_par_iter().map(|exterior_ring| {
        let mut min_lon = f64::MAX;
        let mut max_lon = f64::MIN;
        let mut min_lat = f64::MAX;
        let mut max_lat = f64::MIN;
        
        let mut coords = Vec::with_capacity(exterior_ring.len());
        for &(lon, lat) in &exterior_ring {
            min_lon = min_lon.min(lon);
            max_lon = max_lon.max(lon);
            min_lat = min_lat.min(lat);
            max_lat = max_lat.max(lat);
            coords.push(coord! { x: lon, y: lat });
        }
        let poly = Polygon::new(LineString::new(coords), vec![]);
        
        // Col and row from transform:
        // x = a*col + b*row + c => col = (x - c)/a (assuming b=0)
        // y = d*col + e*row + f => row = (y - f)/e (assuming d=0)
        
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
            return 0;
        }

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
        
        let subset = ArraySubset::new_with_ranges(&[0..1, r_start..r_stop, c_start..c_stop]);
        
        // The array type depends on whether it's raw (f32) or quantized (i16)
        let mut count = 0;
        if array.data_type().to_string() == "float32" || array.data_type().to_string() == "f32" {
            if let Ok(data) = array.retrieve_array_subset_elements::<f32>(&subset) {
                for (i, &val) in arr.iter().enumerate() {
                    if val {
                        if !data[i].is_nan() && data[i] != -32768.0 {
                            count += 1;
                        }
                    }
                }
            }
        } else {
            if let Ok(data) = array.retrieve_array_subset_elements::<i16>(&subset) {
                for (i, &val) in arr.iter().enumerate() {
                    if val {
                        if data[i] != -32768 {
                            count += 1;
                        }
                    }
                }
            }
        }
        count
    }).sum();
    
    Ok(total_count)
}

#[pyfunction]
fn zarrs_polygon_means(store_path: &str, polygons: Vec<Vec<(f64, f64)>>, transform: (f64, f64, f64, f64, f64, f64)) -> PyResult<Vec<f64>> {
    let store = Arc::new(FilesystemStore::new(store_path).unwrap());
    let array = Array::open(store, "/__xarray_dataarray_variable__").unwrap();
    let array_shape = array.shape();
    
    let a = transform.0;
    let b = transform.1;
    let c = transform.2;
    let d = transform.3;
    let e = transform.4;
    let f = transform.5;

    let means: Vec<f64> = polygons.into_par_iter().map(|exterior_ring| {
        let mut min_lon = f64::MAX;
        let mut max_lon = f64::MIN;
        let mut min_lat = f64::MAX;
        let mut max_lat = f64::MIN;
        
        let mut coords = Vec::with_capacity(exterior_ring.len());
        for &(lon, lat) in &exterior_ring {
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
            return f64::NAN;
        }

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
        
        let subset = ArraySubset::new_with_ranges(&[0..1, r_start..r_stop, c_start..c_stop]);
        
        let mut sum = 0f64;
        let mut count = 0;
        
        if array.data_type().to_string() == "float32" || array.data_type().to_string() == "f32" {
            if let Ok(data) = array.retrieve_array_subset_elements::<f32>(&subset) {
                for (i, &val) in arr.iter().enumerate() {
                    if val {
                        if !data[i].is_nan() && data[i] != -32768.0 {
                            sum += data[i] as f64;
                            count += 1;
                        }
                    }
                }
            }
        } else {
            if let Ok(data) = array.retrieve_array_subset_elements::<i16>(&subset) {
                for (i, &val) in arr.iter().enumerate() {
                    if val {
                        if data[i] != -32768 {
                            sum += data[i] as f64;
                            count += 1;
                        }
                    }
                }
            }
        }
        
        if count > 0 {
            sum / (count as f64)
        } else {
            f64::NAN
        }
    }).collect();
    
    Ok(means)
}

#[pymodule]
fn zarrs_plugin(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(zarrs_polygon_query, m)?)?;
    m.add_function(wrap_pyfunction!(zarrs_polygon_means, m)?)?;
    Ok(())
}
