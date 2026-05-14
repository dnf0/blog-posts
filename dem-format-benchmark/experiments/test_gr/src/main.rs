use geo_rasterize::{BinaryRasterizer, Transform};
use geo_types::{Polygon, LineString, coord};
use euclid::Transform2D;

fn main() {
    let poly = Polygon::new(
        LineString::new(vec![
            coord! { x: 8.5f64, y: 46.5 },
            coord! { x: 8.52, y: 46.5 },
            coord! { x: 8.52, y: 46.48 },
            coord! { x: 8.51, y: 46.47 },
            coord! { x: 8.49, y: 46.48 },
            coord! { x: 8.49, y: 46.5 },
            coord! { x: 8.5, y: 46.5 },
        ]),
        vec![],
    );

    let bbox = (8.49f64, 46.47f64, 8.52f64, 46.5f64);
    let col_min = ((bbox.0 + 180.0) * 3600.0).round() as u32;
    let col_max = ((bbox.2 + 180.0) * 3600.0).round() as u32;
    let row_max = ((90.0 - bbox.1) * 3600.0).round() as u32;
    let row_min = ((90.0 - bbox.3) * 3600.0).round() as u32;

    let width = (col_max - col_min + 1) as usize;
    let height = (row_max - row_min + 1) as usize;

    let min_lon = col_min as f64 / 3600.0 - 180.0;
    let max_lat = 90.0 - row_min as f64 / 3600.0;
    let cell_width = 1.0 / 3600.0;
    let cell_height = -1.0 / 3600.0;

    let pix_to_geo: Transform = Transform2D::new(
        cell_width, 0.0,
        0.0, cell_height,
        min_lon, max_lat
    );
    let geo_to_pix = pix_to_geo.inverse().unwrap();
    
    let mut rasterizer = BinaryRasterizer::new(width, height, Some(geo_to_pix)).unwrap();
    rasterizer.rasterize(&poly).unwrap();
    
    let arr = rasterizer.finish();
    let mut count = 0;
    for &val in arr.iter() {
        if val {
            count += 1;
        }
    }
    
    println!("geo-rasterize Count: {}", count);
}
