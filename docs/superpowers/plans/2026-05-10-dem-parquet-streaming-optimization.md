# DEM Parquet Streaming Optimization Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Speed up DEM Parquet creation by replacing row/DataFrame-oriented conversion code with NumPy/Arrow batch streaming.

**Architecture:** Keep `scripts/data_prep.py` as the orchestration entry point, but split conversion helpers into testable functions that emit Arrow tables from NumPy arrays. Raw flat Parquet becomes the batch-native base artifact; indexed variants reuse that stream and use NumPy ordering instead of Pandas chunk materialization.

**Tech Stack:** Python, rasterio, NumPy, PyArrow, pytest, existing h3/s2cell packages

---

## File Structure

- Modify `dem-format-benchmark/scripts/data_prep.py`: add batch-native COG readers, Arrow table builders, timing instrumentation, and NumPy-only sort/reorder helpers; update flat, S2, H3, and GeoParquet builders to use them.
- Modify `dem-format-benchmark/pyproject.toml`: add `pytest` for local verification.
- Create `dem-format-benchmark/tests/test_data_prep_streaming.py`: synthetic-raster tests for coordinate extraction, flat Parquet schema/content, reuse ordering, and NumPy sorting helpers.

## Task 1: Add Focused Streaming Tests

**Files:**
- Create: `dem-format-benchmark/tests/test_data_prep_streaming.py`
- Modify: `dem-format-benchmark/pyproject.toml`

- [ ] **Step 1: Add pytest dependency**

In `dem-format-benchmark/pyproject.toml`, add `pytest>=8.0` to the dependency list:

```toml
    "pytest>=8.0",
```

- [ ] **Step 2: Create the test file**

Create `dem-format-benchmark/tests/test_data_prep_streaming.py`:

```python
import sys
from pathlib import Path

import numpy as np
import pyarrow.parquet as pq
import rasterio
from rasterio.transform import from_origin

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import data_prep


def _write_test_tif(path: Path, data: np.ndarray) -> None:
    transform = from_origin(10.0, 50.0, 0.25, 0.5)
    with rasterio.open(
        path,
        "w",
        driver="GTiff",
        height=data.shape[0],
        width=data.shape[1],
        count=1,
        dtype=str(data.dtype),
        crs="EPSG:4326",
        transform=transform,
        nodata=np.nan,
    ) as dst:
        dst.write(data, 1)


def test_iter_cog_arrow_batches_vectorizes_coordinates(tmp_path):
    tif = tmp_path / "dem.tif"
    data = np.array(
        [
            [1.0, np.nan, 3.0],
            [4.0, 5.0, np.nan],
        ],
        dtype=np.float32,
    )
    _write_test_tif(tif, data)

    batches = list(data_prep._iter_cog_arrow_batches(tif, strip_size=2, sort_hilbert=False))

    assert len(batches) == 1
    table = batches[0]
    assert table.column_names == ["x", "y", "band_value"]
    assert table["x"].to_pylist() == [10.125, 10.625, 10.125, 10.375]
    assert table["y"].to_pylist() == [49.75, 49.75, 49.25, 49.25]
    assert table["band_value"].to_pylist() == [10, 30, 40, 50]


def test_take_arrow_table_reorders_every_column():
    table = data_prep._arrays_to_arrow_table(
        {
            "x": np.array([1.0, 2.0, 3.0], dtype=np.float64),
            "y": np.array([4.0, 5.0, 6.0], dtype=np.float64),
            "band_value": np.array([10, 20, 30], dtype=np.int16),
        }
    )

    reordered = data_prep._take_arrow_table(table, np.array([2, 0, 1], dtype=np.int64))

    assert reordered["x"].to_pylist() == [3.0, 1.0, 2.0]
    assert reordered["y"].to_pylist() == [6.0, 4.0, 5.0]
    assert reordered["band_value"].to_pylist() == [30, 10, 20]


def test_write_batches_to_parquet_reports_rows_size_and_elapsed(tmp_path):
    output = tmp_path / "out.parquet"
    table = data_prep._arrays_to_arrow_table(
        {
            "x": np.array([10.125, 10.625], dtype=np.float64),
            "y": np.array([49.75, 49.75], dtype=np.float64),
            "band_value": np.array([10, 30], dtype=np.int16),
        }
    )

    row_count, size_mb, elapsed = data_prep._write_batches_to_parquet([table], output)

    assert row_count == 2
    assert size_mb > 0
    assert elapsed >= 0
    written = pq.read_table(output)
    assert written.schema.names == ["x", "y", "band_value"]
    assert written["band_value"].to_pylist() == [10, 30]


def test_sample_band_values_for_xy_preserves_raw_order(tmp_path):
    tif = tmp_path / "smooth.tif"
    data = np.array(
        [
            [1.0, 2.0, 3.0],
            [4.0, 5.0, 6.0],
        ],
        dtype=np.float32,
    )
    _write_test_tif(tif, data)

    with rasterio.open(tif) as src:
        sampled = data_prep._sample_band_values_for_xy(
            src,
            np.array([10.625, 10.125, 10.375], dtype=np.float64),
            np.array([49.75, 49.25, 49.25], dtype=np.float64),
        )

    assert sampled.tolist() == [30, 40, 50]
```

- [ ] **Step 3: Run tests and verify they fail because helpers do not exist**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py -q
```

Expected: tests fail with `AttributeError` for `_iter_cog_arrow_batches`, `_arrays_to_arrow_table`, `_take_arrow_table`, or `_sample_band_values_for_xy`.

- [ ] **Step 4: Commit failing tests**

Run:

```bash
git add dem-format-benchmark/pyproject.toml dem-format-benchmark/tests/test_data_prep_streaming.py
git commit -m "test: cover parquet streaming helpers"
```

## Task 2: Add Arrow/NumPy Batch Helpers

**Files:**
- Modify: `dem-format-benchmark/scripts/data_prep.py`
- Test: `dem-format-benchmark/tests/test_data_prep_streaming.py`

- [ ] **Step 1: Add helper functions after `sort_by_hilbert`**

Add this code below `sort_by_hilbert` in `dem-format-benchmark/scripts/data_prep.py`:

```python
def _arrays_to_arrow_table(columns):
    """Build an Arrow table from NumPy arrays using the benchmark schema types."""
    import pyarrow as pa

    arrow_columns = {}
    for name, values in columns.items():
        if name in ("x", "y"):
            arrow_columns[name] = pa.array(values, pa.float64())
        elif name == "band_value":
            arrow_columns[name] = pa.array(values, pa.int16())
        elif name == "s2_cell":
            arrow_columns[name] = pa.array(values, pa.int64())
        elif name == "h3_cell":
            arrow_columns[name] = pa.array(values, pa.uint64())
        else:
            arrow_columns[name] = pa.array(values)
    return pa.table(arrow_columns)


def _take_arrow_table(table, order_idx):
    """Return a table with every column reordered by a NumPy integer index."""
    import pyarrow as pa

    take_idx = pa.array(order_idx.astype(np.int64, copy=False), pa.int64())
    return table.take(take_idx)


def _quantize_band_values(values):
    return np.round(values.astype(np.float32, copy=False) * 10).astype(np.int16)


def _pixel_centers_for_valid_window(transform, window, valid_mask):
    rows, cols = np.nonzero(valid_mask)
    x = transform.c + (cols + window.col_off + 0.5) * transform.a
    y = transform.f + (rows + window.row_off + 0.5) * transform.e
    return x.astype(np.float64, copy=False), y.astype(np.float64, copy=False), rows, cols


def _table_sorted_by_hilbert(table, order=15):
    x = table.column("x").to_numpy()
    y = table.column("y").to_numpy()
    res = 30.0 / 111320.0
    hilbert = _hilbert_indices(
        x, y, REGION_BOUNDS["min_lon"], REGION_BOUNDS["min_lat"], res, res, order,
    )
    return _take_arrow_table(table, np.argsort(hilbert))


def _iter_cog_arrow_batches(cog_path, strip_size=256, sort_hilbert=True):
    """Yield Arrow tables of valid COG pixels without per-pixel Python loops."""
    with rasterio.open(cog_path) as src:
        width, height = src.width, src.height
        transform = src.transform
        for y_start in range(0, height, strip_size):
            y_end = min(y_start + strip_size, height)
            window = Window(0, y_start, width, y_end - y_start)
            data = src.read(1, window=window)
            valid_mask = np.isfinite(data)
            if not valid_mask.any():
                continue

            x, y, rows, cols = _pixel_centers_for_valid_window(transform, window, valid_mask)
            table = _arrays_to_arrow_table({
                "x": x,
                "y": y,
                "band_value": _quantize_band_values(data[rows, cols]),
            })
            yield _table_sorted_by_hilbert(table) if sort_hilbert else table


def _sample_band_values_for_xy(src, x_arr, y_arr):
    """Sample quantized DEM values for existing x/y arrays from an open raster."""
    transform = src.transform
    pixel_width = transform.a
    pixel_height = abs(transform.e)
    origin_x = transform.c
    origin_y = transform.f
    col = ((x_arr - origin_x) / pixel_width).astype(np.int64)
    row = ((origin_y - y_arr) / pixel_height).astype(np.int64)
    col = np.clip(col, 0, src.width - 1)
    row = np.clip(row, 0, src.height - 1)
    values = src.read(1, window=Window(0, 0, src.width, src.height))[row, col]
    return _quantize_band_values(values)
```

- [ ] **Step 2: Update `_write_batches_to_parquet` to return elapsed time**

Replace the function body with:

```python
    import pyarrow as pa
    import pyarrow.parquet as pq

    started = time.time()
    writer = None
    try:
        for tbl in batches_iter:
            if writer is None:
                schema = schema_override or tbl.schema
                col_encoding = {}
                for field in schema:
                    if pa.types.is_integer(field.type):
                        col_encoding[field.name] = "DELTA_BINARY_PACKED"
                writer = pq.ParquetWriter(
                    str(output_path), schema,
                    compression="zstd",
                    compression_level=15,
                    use_dictionary=False,
                    column_encoding=col_encoding,
                    write_statistics=True,
                )
            writer.write_table(tbl)
    finally:
        if writer is not None:
            writer.close()

    elapsed = time.time() - started
    row_count = pq.ParquetFile(str(output_path)).metadata.num_rows
    size_mb = output_path.stat().st_size / (1024 * 1024)
    return row_count, size_mb, elapsed
```

- [ ] **Step 3: Run focused tests**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit helpers**

Run:

```bash
git add dem-format-benchmark/scripts/data_prep.py
git commit -m "feat: add arrow streaming data prep helpers"
```

## Task 3: Convert Flat Parquet Builder to Arrow Streaming

**Files:**
- Modify: `dem-format-benchmark/scripts/data_prep.py`
- Test: `dem-format-benchmark/tests/test_data_prep_streaming.py`

- [ ] **Step 1: Replace raw flat `_batch_generator`**

Inside `_build_flat`, replace the raw variant generator with:

```python
        def _batch_generator():
            yield from _iter_cog_arrow_batches(cog_path, strip_size=256, sort_hilbert=True)

        row_count, size_mb, elapsed = _write_batches_to_parquet(_batch_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")
```

- [ ] **Step 2: Replace non-raw flat sampling**

Inside the non-raw branch of `_build_flat`, remove the full COG read and replace `_sample_generator` with:

```python
        raw_pf = pq.ParquetFile(str(raw_flat_path))

        def _sample_generator():
            with rasterio.open(cog_path) as src:
                for batch in raw_pf.iter_batches(batch_size=500_000):
                    x_arr = batch.column("x").to_numpy()
                    y_arr = batch.column("y").to_numpy()
                    band_int16 = _sample_band_values_for_xy(src, x_arr, y_arr)
                    yield _arrays_to_arrow_table({
                        "x": x_arr,
                        "y": y_arr,
                        "band_value": band_int16,
                    })

        row_count, size_mb, elapsed = _write_batches_to_parquet(_sample_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")
```

- [ ] **Step 3: Remove `_extract_cog_to_dataframe` only after `_build_flat` no longer uses it**

Delete `_extract_cog_to_dataframe` from `data_prep.py` if `rg "_extract_cog_to_dataframe" dem-format-benchmark/scripts/data_prep.py` shows no remaining callers.

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit flat streaming conversion**

Run:

```bash
git add dem-format-benchmark/scripts/data_prep.py
git commit -m "feat: stream flat parquet from arrow batches"
```

## Task 4: Remove Pandas Chunk Assembly from S2 and H3 Builders

**Files:**
- Modify: `dem-format-benchmark/scripts/data_prep.py`
- Test: `dem-format-benchmark/tests/test_data_prep_streaming.py`

- [ ] **Step 1: Add NumPy cell-table helper**

Add this helper near the other Arrow helpers:

```python
def _cell_table_sorted(x_col, y_col, val_col, cell_col, cell_name):
    order_idx = np.argsort(cell_col)
    columns = {
        "x": x_col[order_idx],
        "y": y_col[order_idx],
        "band_value": val_col[order_idx],
        cell_name: cell_col[order_idx],
    }
    return _arrays_to_arrow_table(columns)
```

- [ ] **Step 2: Replace S2 Pandas chunk sort**

Inside raw `_build_s2`, replace the `df_chunk = pd.DataFrame(...)` block and `pa.table(...)` block with:

```python
                tbl = _cell_table_sorted(
                    x_col,
                    y_col,
                    val_col.astype(np.int16, copy=False),
                    s2_cells,
                    "s2_cell",
                )
```

Keep the existing scalar S2 loop in this task, but leave it isolated and visible as the bottleneck.

- [ ] **Step 3: Replace H3 Pandas chunk sort**

Inside raw `_build_h3`, replace the `df_chunk = pd.DataFrame(...)` block and `pa.table(...)` block with:

```python
                tbl = _cell_table_sorted(
                    x_col,
                    y_col,
                    val_col.astype(np.int16, copy=False),
                    cells,
                    "h3_cell",
                )
```

- [ ] **Step 4: Update indexed writer return handling**

For `_build_s2` and `_build_h3`, replace every:

```python
        row_count, size_mb = _write_batches_to_parquet(...)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB)")
```

with:

```python
        row_count, size_mb, elapsed = _write_batches_to_parquet(...)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")
```

- [ ] **Step 5: Run focused tests**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py -q
```

Expected: all tests pass.

- [ ] **Step 6: Commit indexed builder cleanup**

Run:

```bash
git add dem-format-benchmark/scripts/data_prep.py
git commit -m "feat: sort indexed parquet batches with numpy"
```

## Task 5: Add Timing for Spatial Cell Assignment

**Files:**
- Modify: `dem-format-benchmark/scripts/data_prep.py`

- [ ] **Step 1: Add a small timing context manager**

Add near the helper functions:

```python
class _StageTimer:
    def __init__(self, label):
        self.label = label
        self.elapsed = 0.0

    def __enter__(self):
        self._started = time.time()
        return self

    def __exit__(self, exc_type, exc, tb):
        self.elapsed += time.time() - self._started
        return False
```

- [ ] **Step 2: Instrument raw S2 assignment**

Inside raw `_build_s2`, before `_batch_generator`, add:

```python
        assign_timer = _StageTimer("s2 assignment")
        sort_timer = _StageTimer("s2 sort/table")
```

Wrap the scalar loop:

```python
                with assign_timer:
                    s2_cells = np.empty(len(x_col), dtype=np.int64)
                    for i in range(len(x_col)):
                        s2_cells[i] = _cell(y_col[i], x_col[i], S2_LEVEL)
```

Wrap table sorting:

```python
                with sort_timer:
                    tbl = _cell_table_sorted(
                        x_col,
                        y_col,
                        val_col.astype(np.int16, copy=False),
                        s2_cells,
                        "s2_cell",
                    )
```

After writing, print:

```python
        print(f"    s2 assignment: {assign_timer.elapsed:.1f}s; sort/table: {sort_timer.elapsed:.1f}s")
```

- [ ] **Step 3: Instrument raw H3 assignment**

Inside raw `_build_h3`, add the same pattern:

```python
        assign_timer = _StageTimer("h3 assignment")
        sort_timer = _StageTimer("h3 sort/table")
```

Wrap H3 assignment:

```python
                with assign_timer:
                    cells = np.empty(len(x_col), dtype=np.uint64)
                    for i in range(len(x_col)):
                        cells[i] = _f(y_col[i], x_col[i], H3_RESOLUTION)
```

Wrap table sorting:

```python
                with sort_timer:
                    tbl = _cell_table_sorted(
                        x_col,
                        y_col,
                        val_col.astype(np.int16, copy=False),
                        cells,
                        "h3_cell",
                    )
```

After writing, print:

```python
        print(f"    h3 assignment: {assign_timer.elapsed:.1f}s; sort/table: {sort_timer.elapsed:.1f}s")
```

- [ ] **Step 4: Run focused tests**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py -q
```

Expected: all tests pass.

- [ ] **Step 5: Commit instrumentation**

Run:

```bash
git add dem-format-benchmark/scripts/data_prep.py
git commit -m "feat: time parquet index generation stages"
```

## Task 6: Update GeoParquet Return Handling and Remove Unused Pandas Imports Where Possible

**Files:**
- Modify: `dem-format-benchmark/scripts/data_prep.py`

- [ ] **Step 1: Update GeoParquet reuse writer return handling**

In non-raw `_build_geoparquet`, replace:

```python
        row_count, size_mb = _write_batches_to_parquet(_reuse_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB)")
```

with:

```python
        row_count, size_mb, elapsed = _write_batches_to_parquet(_reuse_generator(), output_path)
        print(f"    wrote {row_count:,} rows ({size_mb:.1f} MB) in {elapsed:.1f}s")
```

- [ ] **Step 2: Keep top-level `pandas` only if GeoParquet raw still uses it**

Run:

```bash
cd dem-format-benchmark
rg "pd\\." scripts/data_prep.py
```

Expected: `pd.DataFrame` still appears in raw GeoParquet creation. Keep `import pandas as pd` until GeoParquet raw is separately optimized.

- [ ] **Step 3: Run focused tests**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py -q
```

Expected: all tests pass.

- [ ] **Step 4: Run syntax check**

Run:

```bash
cd dem-format-benchmark
uv run python -m compileall config.py scripts
```

Expected: compileall completes without syntax errors.

- [ ] **Step 5: Commit cleanup**

Run:

```bash
git add dem-format-benchmark/scripts/data_prep.py
git commit -m "chore: align parquet writer call sites"
```

## Task 7: Small End-to-End Smoke Test

**Files:**
- Modify: `dem-format-benchmark/tests/test_data_prep_streaming.py`

- [ ] **Step 1: Add a smoke test for flat build with monkeypatched paths**

Append this test:

```python
def test_build_flat_raw_writes_expected_parquet_for_tiny_cog(tmp_path, monkeypatch):
    tif = tmp_path / "raw.tif"
    out = tmp_path / "flat.parquet"
    data = np.array(
        [
            [1.0, np.nan, 3.0],
            [4.0, 5.0, 6.0],
        ],
        dtype=np.float32,
    )
    _write_test_tif(tif, data)

    monkeypatch.setitem(data_prep.FORMAT_PATHS["cog"], "raw", tif)
    monkeypatch.setitem(data_prep.FORMAT_PATHS["parquet_flat"], "raw", out)

    data_prep._build_flat("raw")

    table = pq.read_table(out)
    assert table.num_rows == 5
    assert table.schema.names == ["x", "y", "band_value"]
    assert sorted(table["band_value"].to_pylist()) == [10, 30, 40, 50, 60]
```

- [ ] **Step 2: Run the smoke test**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py::test_build_flat_raw_writes_expected_parquet_for_tiny_cog -q
```

Expected: test passes.

- [ ] **Step 3: Run all focused tests**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py -q
```

Expected: all tests pass.

- [ ] **Step 4: Commit smoke test**

Run:

```bash
git add dem-format-benchmark/tests/test_data_prep_streaming.py
git commit -m "test: smoke test flat parquet build"
```

## Task 8: Final Verification

**Files:**
- Read: `dem-format-benchmark/scripts/data_prep.py`
- Read: `dem-format-benchmark/tests/test_data_prep_streaming.py`

- [ ] **Step 1: Run focused tests**

Run:

```bash
cd dem-format-benchmark
uv run pytest tests/test_data_prep_streaming.py -q
```

Expected: all tests pass.

- [ ] **Step 2: Run syntax check**

Run:

```bash
cd dem-format-benchmark
uv run python -m compileall config.py scripts tests
```

Expected: compileall completes without syntax errors.

- [ ] **Step 3: Inspect remaining per-row loops**

Run:

```bash
cd dem-format-benchmark
rg -n "for i in range\\(len\\(|iterrows|apply\\(" scripts/data_prep.py
```

Expected: only the explicitly instrumented S2/H3 scalar fallback loops remain.

- [ ] **Step 4: Check git status**

Run:

```bash
git status --short
```

Expected: only intentional changes from this plan are present, plus any unrelated pre-existing user changes that were already in the worktree.
