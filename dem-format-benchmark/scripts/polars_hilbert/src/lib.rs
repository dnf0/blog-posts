use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::derive::polars_expr;

#[inline(always)]
fn hilbert_encode(mut x: u32, mut y: u32) -> u64 {
    let mut d: u64 = 0;
    
    for i in (0..32).rev() {
        let rx = (x >> i) & 1;
        let ry = (y >> i) & 1;
        
        let quad = (3 * rx) ^ ry;
        d += (quad as u64) << (2 * i);
        
        if ry == 0 {
            if rx == 1 {
                // (1 << 32) overflows u32, so we use wrapping or just carefully shift
                let mask = if i == 31 { u32::MAX } else { (1u32 << (i + 1)) - 1 };
                x = x ^ mask;
                y = y ^ mask;
            }
            let t = x;
            x = y;
            y = t;
        }
    }
    d
}

#[inline(always)]
fn get_level(code: u64) -> u8 {
    (code & 0xF) as u8
}

#[inline(always)]
fn get_index(code: u64) -> u64 {
    code >> 4
}

#[inline(always)]
fn make_code(index: u64, level: u8) -> u64 {
    (index << 4) | (level as u64)
}

#[polars_expr(output_type=UInt64)]
fn compute_hilbert(inputs: &[Series]) -> PolarsResult<Series> {
    let cols = inputs[0].u32()?;
    let rows = inputs[1].u32()?;

    let out: UInt64Chunked = cols.into_iter()
        .zip(rows.into_iter())
        .map(|(opt_col, opt_row)| {
            if let (Some(col), Some(row)) = (opt_col, opt_row) {
                Some(make_code(hilbert_encode(col, row), 0))
            } else {
                None
            }
        })
        .collect();

    Ok(out.into_series())
}

#[polars_expr(output_type_func=list_u64_output)]
fn compact_hilbert(inputs: &[Series]) -> PolarsResult<Series> {
    let list_series = &inputs[0];
    let list_chunked = list_series.list()?;

    let mut builder = ListPrimitiveChunkedBuilder::<UInt64Type>::new(
        "compacted_cells".into(),
        list_chunked.len(),
        list_chunked.len() * 4,
        DataType::UInt64,
    );

    for opt_s in list_chunked.into_iter() {
        match opt_s {
            Some(s) => {
                let u64_chunked = s.u64()?;
                let mut levels: Vec<Vec<u64>> = vec![Vec::new(); 16];
                
                levels[0].reserve(u64_chunked.len());
                
                for opt_code in u64_chunked.into_iter() {
                    if let Some(code) = opt_code {
                        levels[get_level(code) as usize].push(get_index(code));
                    }
                }

                for current_level in 0..15 {
                    let mut cur_list = std::mem::take(&mut levels[current_level]);
                    if cur_list.is_empty() {
                        continue;
                    }
                    
                    cur_list.sort_unstable();

                    let mut i = 0;
                    while i < cur_list.len() {
                        if i + 3 < cur_list.len() {
                            let idx0 = cur_list[i];
                            let idx1 = cur_list[i+1];
                            let idx2 = cur_list[i+2];
                            let idx3 = cur_list[i+3];
                            
                            let parent_idx = idx0 >> 2;
                            if idx0 == (parent_idx << 2) | 0 &&
                               idx1 == (parent_idx << 2) | 1 &&
                               idx2 == (parent_idx << 2) | 2 &&
                               idx3 == (parent_idx << 2) | 3 {
                               
                                levels[current_level + 1].push(parent_idx);
                                i += 4;
                                continue;
                            }
                        }
                        levels[current_level].push(cur_list[i]);
                        i += 1;
                    }
                }
                
                let mut out_ids: Vec<u64> = Vec::new();
                for lvl in 0..16 {
                    for &idx in &levels[lvl] {
                        out_ids.push(make_code(idx, lvl as u8));
                    }
                }
                
                builder.append_slice(&out_ids);
            }
            None => {
                builder.append_null();
            }
        }
    }

    Ok(builder.finish().into_series())
}

fn list_u64_output(_: &[Field]) -> PolarsResult<Field> {
    Ok(Field::new("compacted_cells".into(), DataType::List(Box::new(DataType::UInt64))))
}

#[pyfunction]
fn hilbert_cells_for_polygons(polygons: Vec<Vec<(f64, f64)>>) -> PyResult<Vec<u64>> {
    use rayon::prelude::*;
    use geo_rasterize::{BinaryRasterizer, Transform};
    use geo_types::{Polygon, LineString, coord};
    use euclid::Transform2D;
    
    let mut all_expanded: Vec<u64> = polygons.into_par_iter().flat_map(|exterior_ring| {
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
        
        let mut col_min = ((min_lon + 180.0) * 3600.0).round() as u32;
        let mut col_max = ((max_lon + 180.0) * 3600.0).round() as u32;
        let mut row_max = ((90.0 - min_lat) * 3600.0).round() as u32;
        let mut row_min = ((90.0 - max_lat) * 3600.0).round() as u32;
        
        if col_min > col_max { std::mem::swap(&mut col_min, &mut col_max); }
        if row_min > row_max { std::mem::swap(&mut row_min, &mut row_max); }

        let width = (col_max - col_min + 1) as usize;
        let height = (row_max - row_min + 1) as usize;

        let origin_lon = col_min as f64 / 3600.0 - 180.0;
        let origin_lat = 90.0 - row_min as f64 / 3600.0;
        let cell_width = 1.0 / 3600.0;
        let cell_height = -1.0 / 3600.0;

        let pix_to_geo: Transform = Transform2D::new(
            cell_width, 0.0,
            0.0, cell_height,
            origin_lon, origin_lat
        );
        let geo_to_pix = pix_to_geo.inverse().unwrap();
        
        let mut rasterizer = BinaryRasterizer::new(width, height, Some(geo_to_pix)).unwrap();
        rasterizer.rasterize(&poly).unwrap();
        let arr = rasterizer.finish();
        
        let mut d_vals = Vec::new();
        for (i, &val) in arr.iter().enumerate() {
            if val {
                let r = (i / width) as u32;
                let c = (i % width) as u32;
                d_vals.push(hilbert_encode(c + col_min, r + row_min));
            }
        }
        
        let mut local = Vec::new();
        let mut current_level: Vec<u64> = d_vals.into_iter().map(|d| d << 4 | 0).collect();
        local.extend_from_slice(&current_level);
        
        for lvl in 1..16 {
            let mut next_level: Vec<u64> = current_level.into_iter().map(|code| {
                let index = code >> 4;
                (index >> 2) << 4 | (lvl as u64)
            }).collect();
            next_level.sort_unstable();
            next_level.dedup();
            local.extend_from_slice(&next_level);
            current_level = next_level;
        }
        local
    }).collect();
    
    all_expanded.par_sort_unstable();
    all_expanded.dedup();
    Ok(all_expanded)
}

#[pymodule]
fn polars_hilbert(_py: Python, m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(hilbert_cells_for_polygons, m)?)?;
    Ok(())
}
