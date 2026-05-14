import pandas as pd
import plotly.express as px

df = pd.DataFrame({
    "combo": ["A", "A", "B", "B"],
    "data_variant": ["raw", "q100", "raw", "q100"],
    "median_duration_ms": [10, 20, 30000, 40],
    "pattern": ["", "", "/", ""]
})

fig = px.bar(
    df, x="combo", y="median_duration_ms", color="data_variant", pattern_shape="pattern", barmode="group"
)
fig.write_image("test_plot.png")
