"""Stage 3: Generate visualizations, tables, and flame graphs from results using Plotly."""

import json
import subprocess
import sys
from pathlib import Path

import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    DATA_VARIANTS,
    FORMAT_LABELS,
    MEMRAY_DIR,
    PLOTS_DIR,
    QUERY_LABELS,
    RESULTS_DIR,
    TABLES_DIR,
    TOOL_LABELS,
    get_path,
)

def load_results() -> pd.DataFrame:
    path = RESULTS_DIR / "benchmarks.parquet"
    if not path.exists():
        raise FileNotFoundError(f"Results file not found: {path}")

    df = pd.read_parquet(path)
    if "format_label" not in df.columns:
        df["format_label"] = df["format"].map(FORMAT_LABELS)
    if "tool_label" not in df.columns:
        df["tool_label"] = df["tool"].map(TOOL_LABELS)
    if "query_label" not in df.columns:
        df["query_label"] = df["query_type"].map(QUERY_LABELS)
    return df

def figure_storage_size() -> None:
    sizes: list[dict] = []
    for fmt_key in FORMAT_LABELS:
        for variant in DATA_VARIANTS:
            path = get_path(fmt_key, variant)
            if path.exists():
                if path.is_dir():
                    total_size = sum(f.stat().st_size for f in path.rglob('*') if f.is_file())
                    size_mb = total_size / (1024 * 1024)
                else:
                    size_mb = path.stat().st_size / (1024 * 1024)
                sizes.append({
                    "label": f"{FORMAT_LABELS[fmt_key]} [{variant}]",
                    "format": FORMAT_LABELS[fmt_key],
                    "variant": variant,
                    "size_mb": size_mb,
                })

    df = pd.DataFrame(sizes)
    df = df.sort_values(by=["format", "size_mb"], ascending=[True, False])

    fig = px.bar(
        df, 
        x="variant", 
        y="size_mb", 
        color="format",
        barmode="group",
        title="On-disk Storage Size by Format and Variant (Log Scale)",
        labels={"size_mb": "File Size (MiB)", "variant": "Quantization Variant"},
        log_y=True,
    )
    
    fig.update_layout(template="plotly_white", height=600, width=1000)
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out = PLOTS_DIR / "01_storage_size.png"
    fig.write_image(out, scale=2)
    print(f"Saved {out}")

def bar_chart_by_query_type(df: pd.DataFrame, query_type: str, filename: str) -> None:
    subset = df[(df["query_type"] == query_type) & (df["status"] == "success")]
    if subset.empty:
        return

    agg = subset.groupby(["format_label", "tool_label", "data_variant"]).agg(
        median_duration_ms=("duration_ms", "median"),
        median_final_rss_mb=("final_rss_mb", "median"),
    ).reset_index()

    agg["combo"] = agg["format_label"] + " + " + agg["tool_label"]
    agg = agg.sort_values(by="median_duration_ms")

    fig = px.bar(
        agg,
        x="combo",
        y="median_duration_ms",
        color="data_variant",
        barmode="group",
        title=f"{QUERY_LABELS.get(query_type, query_type)} Queries: Median Duration (Log Scale)",
        labels={"median_duration_ms": "Median Duration (ms)", "combo": "Format + Tool"},
        log_y=True,
    )
    fig.update_layout(template="plotly_white", height=600, width=1100, xaxis_tickangle=-45)
    
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out = PLOTS_DIR / filename
    fig.write_image(out, scale=2)
    print(f"Saved {out}")

def _agg_success(df: pd.DataFrame) -> pd.DataFrame:
    success = df[df["status"] == "success"]
    if success.empty:
        return pd.DataFrame()
    agg = (
        success.groupby(["format", "tool", "query_type"])
        .agg(
            median_duration_ms=("duration_ms", "median"),
            median_final_rss_mb=("final_rss_mb", "median"),
            success_rate=("status", lambda s: (s == "success").sum() / len(df.loc[s.index])),
            num_runs=("status", "count"),
        )
        .reset_index()
    )
    agg["format_label"] = agg["format"].map(FORMAT_LABELS)
    agg["tool_label"] = agg["tool"].map(TOOL_LABELS)
    agg["query_label"] = agg["query_type"].map(QUERY_LABELS)
    return agg.sort_values(["query_type", "median_duration_ms"])

def _df_to_md(agg: pd.DataFrame) -> str:
    if agg.empty:
        return "*No successful runs.*\n"
    cols = ["format_label", "tool_label", "query_label", "median_duration_ms", "median_final_rss_mb", "success_rate", "num_runs"]
    available = [c for c in cols if c in agg.columns]
    sub = agg[available].copy()
    sub.columns = ["Format", "Tool", "Query type", "Median duration (ms)", "Median memory (MiB)", "Success rate", "# Runs"][: len(available)]
    return sub.to_markdown(index=False)

def generate_tables(df: pd.DataFrame) -> None:
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    agg = _agg_success(df)

    path = TABLES_DIR / "raw_results.md"
    path.write_text(f"# All results\n\n{_df_to_md(agg)}\n")
    print(f"Saved {path}")

    best_rows: list[dict] = []
    for qt, grp in agg.groupby("query_type"):
        if grp.empty:
            continue
        best_dur = grp.loc[grp["median_duration_ms"].idxmin()]
        best_mem = grp.loc[grp["median_final_rss_mb"].idxmin()]
        best_rows.append({
            "Query type": QUERY_LABELS.get(qt, qt),
            "Fastest combo": f"{best_dur['format_label']} / {best_dur['tool_label']} ({best_dur['median_duration_ms']:.1f} ms)",
            "Lowest-memory combo": f"{best_mem['format_label']} / {best_mem['tool_label']} ({best_mem['median_final_rss_mb']:.1f} MiB)",
        })
    path = TABLES_DIR / "best_per_query.md"
    path.write_text("# Best combo per query type\n\n" + pd.DataFrame(best_rows).to_markdown(index=False) + "\n")
    print(f"Saved {path}")

def plot_scaling_assessment() -> None:
    path = RESULTS_DIR / "scaling.parquet"
    if not path.exists():
        return
        
    df = pd.read_parquet(path)
    
    # Restructure data to show Query Time vs Total Time explicitly
    plot_data = []
    for _, row in df.iterrows():
        if "Rasterio" in row["tool"]:
            plot_data.append({"Architecture": "Rasterio + COG (Total Time)", "Polygons": row["batch_size"], "Seconds": row["total_time_s"]})
        elif "Zarr" in row["tool"]:
            plot_data.append({"Architecture": "Pure Rust Zarr (Total Time)", "Polygons": row["batch_size"], "Seconds": row["total_time_s"]})
        elif "Lance" in row["tool"]:
            plot_data.append({"Architecture": "DuckDB + Lance (Database Query Only)", "Polygons": row["batch_size"], "Seconds": row["query_time_s"]})
        else:
            engine = "Polars" if "Polars" in row["tool"] else "DuckDB"
            # Only plot Polars for clarity
            if engine == "Polars":
                plot_data.append({"Architecture": f"{engine} + Hilbert (Total Time)", "Polygons": row["batch_size"], "Seconds": row["total_time_s"]})
                plot_data.append({"Architecture": f"{engine} + Hilbert (Database Query Only)", "Polygons": row["batch_size"], "Seconds": row["query_time_s"]})
                
    plot_df = pd.DataFrame(plot_data)
    
    fig = px.line(
        plot_df,
        x="Polygons",
        y="Seconds",
        color="Architecture",
        markers=True,
        title="Polygon Batch Extraction Scaling: Execution Time vs Batch Size",
        labels={"Seconds": "Execution Time (seconds)", "Polygons": "Number of Polygons (Batch Size)"},
        log_x=True, # Keep X log to spread out 10, 100, 1000 evenly
        log_y=False, # Use linear Y to show true scaling difference
        color_discrete_map={
            "Rasterio + COG (Total Time)": "red",
            "Polars + Hilbert (Total Time)": "blue",
            "Polars + Hilbert (Database Query Only)": "green",
            "DuckDB + Lance (Database Query Only)": "orange",
            "Pure Rust Zarr (Total Time)": "purple",
        }
    )
    
    # Make the query line dashed
    for d in fig.data:
        if "Query Only" in d.name:
            d.line.dash = 'dash'
    
    fig.update_layout(template="plotly_white", height=600, width=1000)
    out = PLOTS_DIR / "05_scaling_polygons.png"
    fig.write_image(out, scale=2)
    print(f"Saved {out}")

def main() -> None:
    print("=== Stage 3: Visualization ===")
    df = load_results()
    print(f"Loaded {len(df)} rows from benchmarks.parquet")

    figure_storage_size()

    for query_type, filename in [
        ("point", "02_point_latency.png"),
        ("bbox", "03_bbox_latency.png"),
        ("polygon", "04_polygon_latency.png"),
    ]:
        bar_chart_by_query_type(df, query_type, filename)
        
    plot_scaling_assessment()
    generate_tables(df)
    print("\n=== Visualization complete ===")

if __name__ == "__main__":
    main()
