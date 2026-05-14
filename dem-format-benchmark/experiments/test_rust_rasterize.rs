use std::collections::HashMap;

fn rasterize_polygon(exterior_ring: &[(f64, f64)]) -> Vec<(u32, u32)> {
    let mut min_lon = f64::MAX;
    let mut max_lon = f64::MIN;
    let mut min_lat = f64::MAX;
    let mut max_lat = f64::MIN;
    
    let mut vertices = Vec::with_capacity(exterior_ring.len());
    for &(lon, lat) in exterior_ring {
        min_lon = min_lon.min(lon);
        max_lon = max_lon.max(lon);
        min_lat = min_lat.min(lat);
        max_lat = max_lat.max(lat);
        
        let x = (lon + 180.0) * 3600.0;
        let y = (90.0 - lat) * 3600.0;
        vertices.push((x, y));
    }
    
    let mut row_intersections: HashMap<i32, Vec<f64>> = HashMap::new();
    
    let mut j = vertices.len() - 1;
    for i in 0..vertices.len() {
        let (x0, y0) = vertices[j];
        let (x1, y1) = vertices[i];
        j = i;
        
        if (y0 - y1).abs() < 1e-9 {
            continue; // Horizontal edge, skip
        }
        
        let y_min = y0.min(y1);
        let y_max = y0.max(y1);
        
        let r_min = (y_min - 0.5).ceil() as i32;
        let r_max = (y_max - 0.5).floor() as i32;
        
        for r in r_min..=r_max {
            let scan_y = r as f64 + 0.5;
            if scan_y >= y_min && scan_y <= y_max {
                let x_int = x0 + (scan_y - y0) * (x1 - x0) / (y1 - y0);
                row_intersections.entry(r).or_default().push(x_int);
            }
        }
    }
    
    let mut result = Vec::new();
    
    for (&r, ints) in &mut row_intersections {
        ints.sort_unstable_by(|a, b| a.partial_cmp(b).unwrap());
        if ints.len() % 2 != 0 {
            // Edge case: scanline exactly on vertex. Try to filter out.
            // For now, ignore odd intersections issue for simple polygon or just take pairs.
        }
        
        let mut i = 0;
        while i + 1 < ints.len() {
            let x_start = ints[i];
            let x_end = ints[i+1];
            
            let c_min = (x_start - 0.5).ceil() as i32;
            let c_max = (x_end - 0.5).floor() as i32;
            
            for c in c_min..=c_max {
                if r >= 0 && c >= 0 {
                    result.push((c as u32, r as u32));
                }
            }
            i += 2;
        }
    }
    
    result
}

fn main() {
    let polygon = vec![
        (8.5, 46.5),
        (8.52, 46.5),
        (8.52, 46.48),
        (8.51, 46.47),
        (8.49, 46.48),
        (8.49, 46.5),
        (8.5, 46.5)
    ];
    let pixels = rasterize_polygon(&polygon);
    println!("Rust Rasterize Count: {}", pixels.len());
}
