use polars::prelude::*;
use pyo3::prelude::*;
use pyo3_polars::derive::polars_expr;
use s2::cellid::CellID;
use s2::cellunion::CellUnion;
use s2::rect::Rect;
use s2::region::RegionCoverer;

#[polars_expr(output_type_func=list_u64_output)]
fn pixel_to_cells(inputs: &[Series]) -> PolarsResult<Series> {
    let cols = inputs[0].u32()?;
    let rows = inputs[1].u32()?;

    let a = inputs[2].f64()?.get(0).unwrap_or(0.0);
    let b = inputs[3].f64()?.get(0).unwrap_or(0.0);
    let c = inputs[4].f64()?.get(0).unwrap_or(0.0);
    let d = inputs[5].f64()?.get(0).unwrap_or(0.0);
    let e = inputs[6].f64()?.get(0).unwrap_or(0.0);
    let f = inputs[7].f64()?.get(0).unwrap_or(0.0);
    let level = inputs[8].u32()?.get(0).unwrap_or(18) as u8;

    let mut builder = ListPrimitiveChunkedBuilder::<UInt64Type>::new(
        "s2_cells".into(),
        cols.len(),
        cols.len() * 4,
        DataType::UInt64,
    );

    let rc = RegionCoverer {
        min_level: level,
        max_level: level,
        level_mod: 1,
        max_cells: 10,
    };

    for (opt_col, opt_row) in cols.into_iter().zip(rows.into_iter()) {
        if let (Some(col), Some(row)) = (opt_col, opt_row) {
            let col_f = col as f64;
            let row_f = row as f64;

            let lng1 = c + col_f * a + row_f * b;
            let lat1 = f + col_f * d + row_f * e;
            
            let lng2 = c + (col_f + 1.0) * a + (row_f + 1.0) * b;
            let lat2 = f + (col_f + 1.0) * d + (row_f + 1.0) * e;

            let min_lat = lat1.min(lat2);
            let max_lat = lat1.max(lat2);
            let min_lng = lng1.min(lng2);
            let max_lng = lng1.max(lng2);

            let rect = Rect::from_degrees(min_lat, min_lng, max_lat, max_lng);
            let covering = rc.covering(&rect);
            
            let ids: Vec<u64> = covering.0.into_iter().map(|cell| cell.0).collect();
            builder.append_slice(&ids);
        } else {
            builder.append_null();
        }
    }

    Ok(builder.finish().into_series())
}

#[polars_expr(output_type_func=list_u64_output)]
fn compact_cells(inputs: &[Series]) -> PolarsResult<Series> {
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
                let mut cell_ids: Vec<CellID> = u64_chunked
                    .into_no_null_iter()
                    .map(|id| CellID(id))
                    .collect();
                
                let mut cu = CellUnion(cell_ids);
                cu.normalize();
                
                let out_ids: Vec<u64> = cu.0.into_iter().map(|c| c.0).collect();
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
    Ok(Field::new("list_u64".into(), DataType::List(Box::new(DataType::UInt64))))
}

#[pymodule]
fn polars_s2(_py: Python, _m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
