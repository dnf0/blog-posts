import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import os

# Data from our recent benchmark (in milliseconds for readability)
data = {
    "Architecture": [
        "Kerchunk (Virtual Zarr)",
        "Zarr (Physical)",
        "Lance Format",
        "Global VRT",
        "GeoParquet Direct",
        "STAC + TiTiler",
        "Local VRT (Dynamic)"
    ],
    "Mean Time (ms)": [
        0.1136704,
        0.2478913,
        1.1625090,
        2.5790237,
        7.2358643,
        56.8027672,
        95.4579406
    ]
}

df = pd.DataFrame(data)

# Sort by performance
df = df.sort_values(by="Mean Time (ms)")

# Set up the plot aesthetics
sns.set_theme(style="whitegrid", context="paper")
plt.figure(figsize=(10, 6))

# Create the bar plot
# We use a logarithmic scale because the difference is orders of magnitude
ax = sns.barplot(
    x="Mean Time (ms)", 
    y="Architecture", 
    data=df, 
    palette="viridis",
    hue="Architecture",
    legend=False
)

# Apply log scale
ax.set_xscale("log")

# Add labels to the bars
for p in ax.patches:
    width = p.get_width()
    # Format label: show microseconds if < 1ms, otherwise ms
    if width < 1:
        label = f"{width * 1000:.0f} µs"
    else:
        label = f"{width:.1f} ms"
    
    # Position text slightly outside the bar
    ax.text(
        width * 1.1, 
        p.get_y() + p.get_height() / 2, 
        label, 
        ha="left", 
        va="center", 
        fontweight="bold",
        fontsize=10
    )

plt.title("Spatial Slice Retrieval Time (Log Scale)", fontsize=16, pad=20, fontweight="bold")
plt.xlabel("Mean Retrieval Time (ms) [Log Scale]", fontsize=12)
plt.ylabel("")

# Remove top and right borders
sns.despine(left=True, bottom=True)

# Adjust layout and save
plt.tight_layout()
os.makedirs("plots", exist_ok=True)
plt.savefig("plots/vrt_benchmark_results.png", dpi=300, bbox_inches="tight")
print("Plot saved to plots/vrt_benchmark_results.png")
