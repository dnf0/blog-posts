use zarrs::{
    array::{Array, ArraySubset},
    filesystem::FilesystemStore,
};
use std::sync::Arc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let store_path = "../data/dem_q2500.zarr";
    let store = Arc::new(FilesystemStore::new(store_path)?);
    let array = Array::open(store, "/__xarray_dataarray_variable__")?;
    
    let subset = ArraySubset::new_with_ranges(&[0..1, 0..10, 0..10]);
    let data = array.retrieve_array_subset::<Vec<i16>>(&subset);
    println!("Data: {:?}", data.is_ok());
    if let Err(e) = data {
        println!("Error: {:?}", e);
    }
    
    Ok(())
}