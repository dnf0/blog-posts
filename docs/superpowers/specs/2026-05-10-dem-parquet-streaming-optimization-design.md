# DEM Parquet Streaming Optimization Design

## Goal

Speed up DEM Parquet creation from COG inputs by removing row-wise Python work from the hot path. The existing pipeline remains Python-orchestrated, but Parquet creation should operate on NumPy and Arrow batches instead of Pandas rows or per-point Python loops.

## Scope

This design targets `dem-format-benchmark/scripts/data_prep.py`, specifically creation of:

- flat Parquet variants
- Parquet + S2 variants
- Parquet + H3 variants
- GeoParquet reuse where it depends on flat Parquet batches

It does not introduce a Rust CLI. Native code can be revisited later only if S2/H3 indexing remains the dominant cost after batch-native streaming.

## Current Bottlenecks

The current flat path reads raster strips, builds Pandas DataFrames, sorts with Pandas, converts back to Arrow, then writes Parquet. The indexed paths add a worse problem: raw S2 and H3 variants compute one spatial cell per row in Python loops over hundreds of millions of pixels.

Recent reuse changes help non-raw variants, but the raw indexed variants still pay the per-row index cost. Flat creation also still does unnecessary DataFrame materialization.

## Architecture

### Batch-Native COG Reader

Replace `_extract_cog_to_dataframe()` with an iterator that yields Arrow-ready NumPy arrays:

- read a raster window with `rasterio`
- compute valid mask with `np.isfinite`
- compute pixel center coordinates from the affine transform using vectorized row and column arrays
- quantize `band_value` to `int16` before Arrow conversion
- return Arrow `Table` or `RecordBatch` objects directly

The iterator should avoid Python loops over pixels. Loops over windows or row blocks are acceptable.

### Arrow Parquet Writer

Keep `_write_batches_to_parquet()` as the central writer, but feed it Arrow-native batches. Preserve current compression choices:

- `compression="zstd"`
- `compression_level=15`
- `use_dictionary=False`
- delta encoding for integer columns where PyArrow supports it
- statistics enabled

The writer should expose row count, file size, and elapsed time for each output.

### Flat Parquet

Raw flat Parquet becomes the base artifact:

- stream COG windows
- compute x/y/value arrays vectorized
- optionally sort each batch by Hilbert index using NumPy only
- write Arrow batches directly

Smoothed flat variants should continue to reuse raw flat x/y ordering and swap `band_value` from the matching smoothed COG. The implementation should avoid loading the whole smoothed COG if memory or IO shows it is a problem; batch/window sampling is preferred when straightforward.

### H3 Parquet

For raw H3:

- first try the fastest vectorized API available in the installed `h3` package
- if no vectorized API is available, use a batch-level cache keyed by pixel coordinate or quantized lon/lat to avoid repeated Python calls where possible
- sort batches with NumPy by `h3_cell`
- write Arrow batches directly

For non-raw H3:

- reuse raw H3 x/y/h3 ordering
- replace only `band_value` from the corresponding flat variant

### S2 Parquet

For raw S2:

- use a vectorized S2 implementation only if one is already available in the environment
- otherwise isolate the current scalar S2 fallback behind a clearly named function and instrument it
- do not hide a per-row Python loop inside the main streaming path

For non-raw S2:

- reuse raw S2 x/y/s2 ordering
- replace only `band_value` from the corresponding flat variant

If S2 has no practical vectorized path locally, S2 raw creation remains the only accepted slow path and should be reported separately.

### GeoParquet

GeoParquet raw creation still needs geometry construction, but it should consume flat batches without extra Pandas round trips where possible. Non-raw GeoParquet should keep reusing raw geometry and swapping `band_value`.

## Instrumentation

Add timing around each substage:

- COG window read
- coordinate/value batch construction
- Hilbert or cell sorting
- S2/H3 cell assignment
- Parquet write

The output should make it obvious whether runtime is dominated by raster IO, compression, sorting, or spatial indexing.

## Testing

Use a small synthetic COG or a small window from the real COG to verify:

- output schemas match existing benchmark expectations
- row counts match previous outputs for the same source window
- flat x/y/value values match the raster source
- non-raw variants preserve raw x/y and index ordering while changing only `band_value`
- generated Parquet files can be queried by the existing benchmark code

Full-scale timing should be treated as a manual benchmark because the complete data set is large.

## Future Work

If S2/H3 assignment remains too slow after this change, add a native vectorized Python extension or Polars plugin for cell assignment. That should be a focused follow-up, not part of the first streaming optimization.
