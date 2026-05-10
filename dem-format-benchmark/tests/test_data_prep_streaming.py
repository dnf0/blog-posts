import sys
from pathlib import Path

import numpy as np
import pytest
import pyarrow.parquet as pq
import rasterio
from rasterio.transform import from_origin

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from scripts import data_prep


def _write_test_tif(path: Path, data: np.ndarray, nodata=np.nan) -> None:
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
        nodata=nodata,
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


def test_iter_cog_arrow_batches_excludes_finite_nodata(tmp_path):
    tif = tmp_path / "dem_nodata.tif"
    data = np.array(
        [
            [1.0, -32767.0, 3.0],
            [-32767.0, 5.0, 6.0],
        ],
        dtype=np.float32,
    )
    _write_test_tif(tif, data, nodata=-32767.0)

    batches = list(data_prep._iter_cog_arrow_batches(tif, strip_size=2, sort_hilbert=False))

    assert len(batches) == 1
    assert batches[0]["band_value"].to_pylist() == [10, 30, 50, 60]


def test_quantize_band_values_preserves_high_elevations_without_overflow():
    quantized = data_prep._quantize_band_values(np.array([4000.0], dtype=np.float32))

    assert quantized.dtype == np.int32
    assert quantized.tolist() == [40000]


def test_take_arrow_table_reorders_every_column():
    table = data_prep._arrays_to_arrow_table(
        {
            "x": np.array([1.0, 2.0, 3.0], dtype=np.float64),
            "y": np.array([4.0, 5.0, 6.0], dtype=np.float64),
            "band_value": np.array([10, 20, 30], dtype=np.int32),
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
            "band_value": np.array([10, 30], dtype=np.int32),
        }
    )

    row_count, size_mb, elapsed = data_prep._write_batches_to_parquet([table], output)

    assert row_count == 2
    assert size_mb > 0
    assert elapsed >= 0
    written = pq.read_table(output)
    assert written.schema.names == ["x", "y", "band_value"]
    assert written["band_value"].to_pylist() == [10, 30]


def test_write_batches_to_parquet_rejects_empty_input(tmp_path):
    output = tmp_path / "empty.parquet"

    with pytest.raises(ValueError, match="no batches to write"):
        data_prep._write_batches_to_parquet([], output)


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
