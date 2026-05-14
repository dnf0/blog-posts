import pandas as pd
import plotly.express as px

df = pd.DataFrame({
    "combo": ["A", "A", "B", "B"],
    "data_variant": ["raw", "q100", "raw", "q100"],
    "median_duration_ms": [10, 20, 30000, 40],
    "is_timeout": ["success", "success", "timeout", "success"]
})

fig = px.bar(
    df, x="combo", y="median_duration_ms", color="data_variant", barmode="group"
)

for trace in fig.data:
    variant = trace.name
    patterns = []
    for x in trace.x:
        # find corresponding row
        match = df[(df["combo"] == x) & (df["data_variant"] == variant)]
        if not match.empty and match["is_timeout"].iloc[0] == "timeout":
            patterns.append("/")
        else:
            patterns.append("")
    trace.marker.pattern = dict(shape=patterns)

fig.write_image("test_plot2.png")
