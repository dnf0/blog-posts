import pyarrow.parquet as pq
from pathlib import Path

def is_parquet_valid(path: Path) -> bool:
    if not path.exists():
        return False
    try:
        pq.ParquetFile(str(path))
        return True
    except Exception:
        return False

path = Path("dem-format-benchmark/data/dem_s2.parquet")
print(f"File {path} exists: {path.exists()}")
print(f"File {path} valid: {is_parquet_valid(path)}")

# Create a dummy corrupted parquet
dummy = Path("dummy_corrupt.parquet")
dummy.write_text("not a parquet")
print(f"Dummy valid: {is_parquet_valid(dummy)}")
dummy.unlink()
