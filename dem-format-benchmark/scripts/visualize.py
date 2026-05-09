"""Stage 3: Generate visualizations, tables, and flame graphs from results."""

import json
import subprocess
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")

import matplotlib.pyplot as plt  # noqa: E402
import numpy as np  # noqa: E402
import pandas as pd  # noqa: E402
import seaborn as sns  # noqa: E402

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (  # noqa: E402
    FORMAT_LABELS,
    FORMAT_PATHS,
    MEMRAY_DIR,
    PLOTS_DIR,
    QUERY_LABELS,
    RESULTS_DIR,
    TABLES_DIR,
    TOOL_LABELS,
)

# ---------------------------------------------------------------------------
# Matplotlib style
# ---------------------------------------------------------------------------
plt.rcParams.update(
    {
        "figure.dpi": 150,
        "savefig.dpi": 150,
        "font.family": "sans-serif",
        "font.sans-serif": [
            "DejaVu Sans",
            "Helvetica",
            "Arial",
            "sans-serif",
        ],
    }
)

FORMAT_MARKERS = {
    "cog": "o",
    "parquet_flat": "s",
    "parquet_s2": "D",
    "parquet_h3": "^",
    "geoparquet": "v",
}

QUERY_MARKERS = {
    "point": "o",
    "bbox": "s",
    "polygon": "D",
}


# ======================================================================
# Data loading
# ======================================================================


def load_results() -> pd.DataFrame:
    """Load ``benchmarks.parquet`` and ensure label columns are present."""
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


# ======================================================================
# Storage size figure
# ======================================================================


def figure_storage_size() -> None:
    """Horizontal bar chart of on-disk file sizes, saved as *01_storage_size.png*."""
    sizes: dict[str, float] = {}
    for fmt_key, fmt_path in FORMAT_PATHS.items():
        if fmt_path.exists():
            sizes[fmt_key] = fmt_path.stat().st_size / (1024 * 1024)
        else:
            sizes[fmt_key] = 0.0

    items = sorted(sizes.items(), key=lambda x: x[1], reverse=True)
    labels = [FORMAT_LABELS[k] for k, _ in items]
    values = [v for _, v in items]
    colors = sns.color_palette("viridis", len(labels))

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(labels, values, color=colors)
    ax.set_xlabel("File size (MiB)")
    ax.set_title("On-disk storage size by format")
    ax.invert_yaxis()

    for bar, val in zip(bars, values):
        ax.text(
            bar.get_width() + max(values) * 0.01,
            bar.get_y() + bar.get_height() / 2,
            f"{val:.1f} MiB",
            va="center",
            fontsize=9,
        )

    plt.tight_layout()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    fig.savefig(PLOTS_DIR / "01_storage_size.png")
    plt.close(fig)
    print(f"Saved {PLOTS_DIR / '01_storage_size.png'}")


# ======================================================================
# Per-query-type scatter plots
# ======================================================================


def scatter_by_query_type(
    df: pd.DataFrame, query_type: str, filename: str
) -> None:
    """Scatter of median memory vs median duration for one query type.

    Each format gets a different marker.  Saves to *filename* inside
    ``PLOTS_DIR``.
    """
    subset = df[
        (df["query_type"] == query_type) & (df["status"] == "success")
    ]
    if subset.empty:
        print(f"  No successful runs for query_type={query_type}, skipping {filename}")
        return

    agg = (
        subset.groupby(["format", "format_label", "tool", "tool_label"])
        .agg(
            median_duration_ms=("duration_ms", "median"),
            median_final_rss_mb=("final_rss_mb", "median"),
        )
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(9, 6))

    for fmt_key in sorted(agg["format"].unique()):
        rows = agg[agg["format"] == fmt_key]
        if rows.empty:
            continue
        marker = FORMAT_MARKERS.get(fmt_key, "o")
        label = rows["format_label"].iloc[0]
        # Offset tool label slightly so text doesn't overlap the marker
        for _, r in rows.iterrows():
            ax.scatter(
                r["median_duration_ms"],
                r["median_final_rss_mb"],
                marker=marker,
                s=80,
                label=label,
                edgecolors="black",
                linewidth=0.5,
            )
            ax.annotate(
                r["tool_label"],
                (r["median_duration_ms"], r["median_final_rss_mb"]),
                textcoords="offset points",
                xytext=(6, 4),
                fontsize=7,
                alpha=0.85,
            )

    # Deduplicate legend entries
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys(), fontsize=8, loc="best")

    ax.set_xlabel("Median duration (ms)")
    ax.set_ylabel("Median final RSS (MiB)")
    ax.set_title(f"{QUERY_LABELS.get(query_type, query_type)} queries")
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)

    plt.tight_layout()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out = PLOTS_DIR / filename
    fig.savefig(out)
    plt.close(fig)
    print(f"Saved {out}")


# ======================================================================
# Combined scatter with Pareto frontier
# ======================================================================


def figure_combined_scatter(df: pd.DataFrame) -> None:
    """All query types combined, marker per query type, with Pareto frontier.

    Saves to *05_combined_scatter.png*.
    """
    success = df[df["status"] == "success"]
    if success.empty:
        print("  No successful runs, skipping combined scatter.")
        return

    agg = (
        success.groupby(["format", "format_label", "query_type", "query_label"])
        .agg(
            median_duration_ms=("duration_ms", "median"),
            median_final_rss_mb=("final_rss_mb", "median"),
        )
        .reset_index()
    )

    fig, ax = plt.subplots(figsize=(10, 7))

    for qt in sorted(agg["query_type"].unique()):
        rows = agg[agg["query_type"] == qt]
        marker = QUERY_MARKERS.get(qt, "o")
        label = QUERY_LABELS.get(qt, qt)
        ax.scatter(
            rows["median_duration_ms"],
            rows["median_final_rss_mb"],
            marker=marker,
            s=90,
            label=label,
            edgecolors="black",
            linewidth=0.5,
        )
        for _, r in rows.iterrows():
            ax.annotate(
                r["format_label"],
                (r["median_duration_ms"], r["median_final_rss_mb"]),
                textcoords="offset points",
                xytext=(6, 4),
                fontsize=6.5,
                alpha=0.8,
            )

    # ---- Pareto frontier (lower-left envelope) ----
    sorted_agg = agg.sort_values("median_duration_ms")
    frontier_x: list[float] = []
    frontier_y: list[float] = []
    best_y = float("inf")
    for _, row in sorted_agg.iterrows():
        if row["median_final_rss_mb"] < best_y:
            frontier_x.append(row["median_duration_ms"])
            frontier_y.append(row["median_final_rss_mb"])
            best_y = row["median_final_rss_mb"]

    if len(frontier_x) >= 2:
        ax.plot(
            frontier_x,
            frontier_y,
            "k--",
            linewidth=1.2,
            alpha=0.6,
            label="Pareto frontier",
        )
    elif len(frontier_x) == 1:
        ax.scatter(
            frontier_x[0],
            frontier_y[0],
            marker="*",
            s=200,
            color="black",
            label="Pareto frontier (single)",
        )

    ax.legend(fontsize=8, loc="best")
    ax.set_xlabel("Median duration (ms)")
    ax.set_ylabel("Median final RSS (MiB)")
    ax.set_title("Memory vs duration across all query types")
    ax.set_xlim(left=0)
    ax.set_ylim(bottom=0)

    plt.tight_layout()
    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out = PLOTS_DIR / "05_combined_scatter.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"Saved {out}")


# ======================================================================
# Decision heatmap
# ======================================================================


def figure_decision_heatmap(df: pd.DataFrame) -> None:
    """Format x tool viability heatmap with annotations.

    Viability = ``success_rate >= 0.8 AND median_memory < 2000``.
    Cells are annotated GOOD / OK / POOR / FAIL / N/A.

    Saves to *06_decision_heatmap.png*.
    """
    stats: list[dict] = []
    for (fmt, tool), grp in df.groupby(["format", "tool"]):
        total = len(grp)
        success_count = (grp["status"] == "success").sum()
        success_rate = success_count / total if total > 0 else 0.0
        median_memory = (
            grp.loc[grp["status"] == "success", "final_rss_mb"].median()
            if success_count > 0
            else float("nan")
        )
        stats.append(
            {
                "format": fmt,
                "tool": tool,
                "success_rate": success_rate,
                "median_memory": median_memory,
                "total_runs": total,
                "successful_runs": success_count,
            }
        )

    stats_df = pd.DataFrame(stats)

    def classify(row: pd.Series) -> str:
        if row["successful_runs"] == 0:
            return "N/A"
        if row["success_rate"] >= 0.8 and row["median_memory"] < 2000:
            if row["median_memory"] < 500:
                return "GOOD"
            return "OK"
        if row["success_rate"] >= 0.5:
            return "POOR"
        return "FAIL"

    stats_df["label"] = stats_df.apply(classify, axis=1)

    score_map = {"GOOD": 4, "OK": 3, "POOR": 2, "FAIL": 1, "N/A": 0}
    stats_df["score"] = stats_df["label"].map(score_map)

    pivot = stats_df.pivot_table(
        index="format",
        columns="tool",
        values="score",
        aggfunc="first",
    )
    pivot_labels = stats_df.pivot_table(
        index="format",
        columns="tool",
        values="label",
        aggfunc="first",
    )

    # Ensure consistent ordering
    fmt_order = list(FORMAT_LABELS.keys())
    tool_order = list(TOOL_LABELS.keys())
    pivot = pivot.reindex(index=fmt_order, columns=tool_order)
    pivot_labels = pivot_labels.reindex(index=fmt_order, columns=tool_order)

    pivot.index = [FORMAT_LABELS.get(k, k) for k in pivot.index]
    pivot.columns = [TOOL_LABELS.get(k, k) for k in pivot.columns]
    pivot_labels.index = pivot.index
    pivot_labels.columns = pivot.columns

    annot = pivot_labels.fillna("N/A").values

    fig, ax = plt.subplots(figsize=(12, 6))
    sns.heatmap(
        pivot,
        annot=annot,
        fmt="",
        cmap="RdYlGn",
        vmin=0,
        vmax=4,
        linewidths=1,
        linecolor="white",
        cbar_kws={"ticks": [0.5, 1.5, 2.5, 3.5, 4]},
        ax=ax,
    )
    # Relabel colorbar
    cbar = ax.collections[0].colorbar
    if cbar is not None:
        cbar.set_ticks([0.4, 1.2, 2.0, 2.8, 3.6])
        cbar.set_ticklabels(["N/A", "FAIL", "POOR", "OK", "GOOD"])

    ax.set_title("Format x tool viability heatmap")
    plt.tight_layout()

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)
    out = PLOTS_DIR / "06_decision_heatmap.png"
    fig.savefig(out)
    plt.close(fig)
    print(f"Saved {out}")


# ======================================================================
# Markdown / JSON tables
# ======================================================================


def _agg_success(df: pd.DataFrame) -> pd.DataFrame:
    """Return one row per (format, tool, query_type) with median metrics."""
    success = df[df["status"] == "success"]
    if success.empty:
        return pd.DataFrame()
    agg = (
        success.groupby(["format", "tool", "query_type"])
        .agg(
            median_duration_ms=("duration_ms", "median"),
            median_final_rss_mb=("final_rss_mb", "median"),
            success_rate=(
                "status",
                lambda s: (s == "success").sum() / len(df.loc[s.index]),
            ),
            num_runs=("status", "count"),
        )
        .reset_index()
    )
    agg["format_label"] = agg["format"].map(FORMAT_LABELS)
    agg["tool_label"] = agg["tool"].map(TOOL_LABELS)
    agg["query_label"] = agg["query_type"].map(QUERY_LABELS)
    return agg.sort_values(["query_type", "median_duration_ms"])


def _df_to_md(agg: pd.DataFrame) -> str:
    """Convert aggregated DataFrame to markdown table string."""
    if agg.empty:
        return "*No successful runs.*\n"
    cols = ["format_label", "tool_label", "query_label",
            "median_duration_ms", "median_final_rss_mb", "success_rate", "num_runs"]
    available = [c for c in cols if c in agg.columns]
    sub = agg[available].copy()
    sub.columns = [
        "Format", "Tool", "Query type",
        "Median duration (ms)", "Median memory (MiB)",
        "Success rate", "# Runs",
    ][: len(available)]
    return sub.to_markdown(index=False)


def generate_tables(df: pd.DataFrame) -> None:
    """Create markdown summary tables and a file-sizes JSON."""
    TABLES_DIR.mkdir(parents=True, exist_ok=True)
    agg = _agg_success(df)

    # --- raw_results.md ---
    path = TABLES_DIR / "raw_results.md"
    path.write_text(f"# All results\n\n{_df_to_md(agg)}\n")
    print(f"Saved {path}")

    # --- memory-filtered tables ---
    for threshold in (500, 1000, 2000):
        filtered = agg[agg["median_final_rss_mb"] < threshold]
        path = TABLES_DIR / f"memory_under_{threshold}mb.md"
        path.write_text(
            f"# Combos with median memory < {threshold} MiB\n\n"
            f"{_df_to_md(filtered)}\n"
        )
        print(f"Saved {path}")

    # --- best_per_query.md ---
    best_rows: list[dict] = []
    for qt, grp in agg.groupby("query_type"):
        if grp.empty:
            continue
        best_dur = grp.loc[grp["median_duration_ms"].idxmin()]
        best_mem = grp.loc[grp["median_final_rss_mb"].idxmin()]
        best_rows.append(
            {
                "Query type": QUERY_LABELS.get(qt, qt),
                "Fastest combo": (
                    f"{best_dur['format_label']} / {best_dur['tool_label']} "
                    f"({best_dur['median_duration_ms']:.1f} ms)"
                ),
                "Lowest-memory combo": (
                    f"{best_mem['format_label']} / {best_mem['tool_label']} "
                    f"({best_mem['median_final_rss_mb']:.1f} MiB)"
                ),
            }
        )
    path = TABLES_DIR / "best_per_query.md"
    path.write_text(
        "# Best combo per query type\n\n"
        + pd.DataFrame(best_rows).to_markdown(index=False)
        + "\n"
    )
    print(f"Saved {path}")

    # --- file_sizes.json ---
    file_sizes: dict[str, float] = {}
    for fmt_key, fmt_path in FORMAT_PATHS.items():
        if fmt_path.exists():
            file_sizes[FORMAT_LABELS.get(fmt_key, fmt_key)] = round(
                fmt_path.stat().st_size / (1024 * 1024), 2
            )
        else:
            file_sizes[FORMAT_LABELS.get(fmt_key, fmt_key)] = 0.0

    path = TABLES_DIR / "file_sizes.json"
    path.write_text(json.dumps(file_sizes, indent=2) + "\n")
    print(f"Saved {path}")


# ======================================================================
# Memray flame graphs
# ======================================================================


def generate_flame_graphs(df: pd.DataFrame) -> None:
    """For each query type's best (lowest median duration) combo, generate a
    memray flame graph PNG.  Uses the median-duration run's ``.bin`` report."""
    success = df[df["status"] == "success"]
    if success.empty:
        print("  No successful runs, skipping flame graphs.")
        return

    PLOTS_DIR.mkdir(parents=True, exist_ok=True)

    query_map = {"point": "02", "bbox": "03", "polygon": "04"}

    for query_type in ["point", "bbox", "polygon"]:
        qt_data = success[success["query_type"] == query_type]
        if qt_data.empty:
            print(f"  No data for {query_type}, skipping flame graph.")
            continue

        # Find best (format, tool) combo by median duration
        combo_medians = (
            qt_data.groupby(["format", "tool"])["duration_ms"]
            .median()
            .sort_values()
        )
        if combo_medians.empty:
            continue
        best_fmt, best_tool = combo_medians.index[0]
        best_median = combo_medians.iloc[0]

        # Find the specific run closest to the median duration
        combo_runs = qt_data[
            (qt_data["format"] == best_fmt) & (qt_data["tool"] == best_tool)
        ].copy()
        combo_runs["dist_from_median"] = (
            combo_runs["duration_ms"] - best_median
        ).abs()
        best_run = combo_runs.loc[combo_runs["dist_from_median"].idxmin()]
        run_idx = int(best_run["run"])

        # Build report path
        report_path = (
            MEMRAY_DIR
            / f"{best_fmt}__{best_tool}__{query_type}__run{run_idx:02d}.bin"
        )

        if not report_path.exists():
            print(
                f"  Memray report not found: {report_path}, "
                f"skipping flame graph for {query_type}"
            )
            continue

        prefix = query_map.get(query_type, query_type)
        output_path = PLOTS_DIR / f"{prefix}_{query_type}_flamegraph.png"

        print(
            f"  Generating flame graph for {query_type}: "
            f"{best_fmt}/{best_tool} (run {run_idx})"
        )
        subprocess.run(
            [
                sys.executable,
                "-m",
                "memray",
                "flamegraph",
                "-o",
                str(output_path),
                str(report_path),
            ],
            check=True,
            timeout=120,
        )
        print(f"Saved {output_path}")


# ======================================================================
# Main
# ======================================================================


def main() -> None:
    """Run all visualisation steps in order."""
    print("=== Stage 3: Visualization ===")

    df = load_results()
    print(f"Loaded {len(df)} rows from benchmarks.parquet")

    # 1. Storage size
    print("\n[1/8] Storage size...")
    figure_storage_size()

    # 2-4. Per-query-type scatter plots
    for query_type, filename in [
        ("point", "02_point_scatter.png"),
        ("bbox", "03_bbox_scatter.png"),
        ("polygon", "04_polygon_scatter.png"),
    ]:
        print(f"\n[scatter] {query_type}...")
        scatter_by_query_type(df, query_type, filename)

    # 5. Flame graphs
    print("\n[5/8] Flame graphs...")
    generate_flame_graphs(df)

    # 6. Combined scatter
    print("\n[6/8] Combined scatter...")
    figure_combined_scatter(df)

    # 7. Decision heatmap
    print("\n[7/8] Decision heatmap...")
    figure_decision_heatmap(df)

    # 8. Tables
    print("\n[8/8] Tables...")
    generate_tables(df)

    print("\n=== Visualization complete ===")


if __name__ == "__main__":
    main()
