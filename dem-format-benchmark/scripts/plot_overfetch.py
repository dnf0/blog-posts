import plotly.express as px
import pandas as pd
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

data = [
    {"Format": "Zarr (64x64 chunks)", "Data Fetched (MB)": 0.07, "Architecture": "2D Spatial Chunking"},
    {"Format": "Zarr (128x128 chunks)", "Data Fetched (MB)": 0.12, "Architecture": "2D Spatial Chunking"},
    {"Format": "Zarr (512x512 chunks)", "Data Fetched (MB)": 0.50, "Architecture": "2D Spatial Chunking"},
    {"Format": "Parquet (Row Groups)", "Data Fetched (MB)": 689.38, "Architecture": "1D Columnar (Zone Map Trap)"}
]

df = pd.DataFrame(data)

fig = px.bar(
    df,
    x="Format",
    y="Data Fetched (MB)",
    color="Architecture",
    title="The Read Amplification Paradox: Data Fetched for a 23 KB Polygon",
    text="Data Fetched (MB)",
    log_y=True,
    color_discrete_map={
        "2D Spatial Chunking": "blue",
        "1D Columnar (Zone Map Trap)": "red"
    }
)

fig.update_traces(texttemplate='%{text} MB', textposition='outside')
fig.update_layout(template="plotly_white", height=600, width=900, yaxis_title="Uncompressed Data Fetched (MB) - Log Scale")

out = Path("plots/06_overfetch_comparison.png")
fig.write_image(out, scale=2)
print(f"Saved {out}")
