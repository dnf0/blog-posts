import rasterio
import matplotlib.pyplot as plt
from rasterio.plot import show
from pathlib import Path
import sys

# Load COG paths
data_dir = Path("data")
raw_path = data_dir / "dem_cog.tif"
q100_path = data_dir / "dem_cog_q100.tif"
q1000_path = data_dir / "dem_cog_q1000.tif"
q2500_path = data_dir / "dem_cog_q2500.tif"

# Define a window to read (e.g. 1000x1000 pixels in the middle)
# Total size is ~22k x 22k. Let's pick a scenic window in the Alps.
# Roughly center:
window = rasterio.windows.Window(10000, 10000, 1000, 1000)

with rasterio.open(raw_path) as src:
    raw_data = src.read(1, window=window)

with rasterio.open(q100_path) as src:
    q100_data = src.read(1, window=window)

with rasterio.open(q1000_path) as src:
    q1000_data = src.read(1, window=window)

with rasterio.open(q2500_path) as src:
    q2500_data = src.read(1, window=window)

fig, axes = plt.subplots(1, 4, figsize=(20, 5))

# Plot raw
im0 = axes[0].imshow(raw_data, cmap='terrain')
axes[0].set_title('Raw DEM (Continuous)')
axes[0].axis('off')

# Plot q100
im1 = axes[1].imshow(q100_data, cmap='terrain')
axes[1].set_title('q100 (100m Quantization)')
axes[1].axis('off')

# Plot q1000
im2 = axes[2].imshow(q1000_data, cmap='terrain')
axes[2].set_title('q1000 (1000m Quantization)')
axes[2].axis('off')

# Plot q2500
im3 = axes[3].imshow(q2500_data, cmap='terrain')
axes[3].set_title('q2500 (2500m Quantization)')
axes[3].axis('off')

plt.tight_layout()
out_path = Path("plots/00_quantization_visual.png")
out_path.parent.mkdir(parents=True, exist_ok=True)
plt.savefig(out_path, dpi=150, bbox_inches='tight')
print(f"Saved {out_path}")
