"""Stage 4: Assemble the MDX blog post from template + benchmark results."""

import json
import sys
import shutil
from datetime import date
from pathlib import Path

import pandas as pd
from jinja2 import Environment, FileSystemLoader

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from config import (
    ROOT, RESULTS_DIR, PLOTS_DIR, TABLES_DIR, CONTENT_DIR,
    FORMAT_LABELS, TOOL_LABELS, QUERY_LABELS, DATA_VARIANTS, VARIANT_LABELS,
    get_path,
)


def load_context():
    """Build the template context from benchmark results and generated assets."""
    df = pd.read_parquet(RESULTS_DIR / "benchmarks.parquet")
    df["format_label"] = df["format"].map(FORMAT_LABELS)
    df["tool_label"] = df["tool"].map(TOOL_LABELS)
    df["query_label"] = df["query_type"].map(QUERY_LABELS)

    today = date.today().isoformat()

    # Filter to successful runs
    success = df[df["status"] == "success"]

    agg = success.groupby(["format", "tool", "query_type"]).agg(
        median_duration_ms=("duration_ms", "median"),
        median_final_rss_mb=("final_rss_mb", "median"),
    ).reset_index()
    agg["Format"] = agg["format"].map(FORMAT_LABELS)
    agg["Tool"] = agg["tool"].map(TOOL_LABELS)
    agg["Query"] = agg["query_type"].map(QUERY_LABELS)
    agg["Duration (ms)"] = agg["median_duration_ms"].round(1)
    agg["Memory (MB)"] = agg["median_final_rss_mb"].round(1)

    # Best per query type
    best = {}
    for qt in ["point", "bbox", "polygon"]:
        qdf = agg[agg["query_type"] == qt].sort_values("median_duration_ms")
        best[qt] = qdf.head(3).to_dict("records")

    # File sizes (raw variant for baseline comparison)
    sizes = {}
    for fmt_key in FORMAT_LABELS:
        path = get_path(fmt_key, "raw")
        if path.exists():
            sizes[FORMAT_LABELS[fmt_key]] = round(path.stat().st_size / (1024 * 1024), 1)

    # Build file sizes markdown table (all variants)
    file_sizes_rows = []
    for fmt_key in FORMAT_LABELS:
        for variant in DATA_VARIANTS:
            path = get_path(fmt_key, variant)
            if path.exists():
                size_mb = round(path.stat().st_size / (1024 * 1024), 1)
                file_sizes_rows.append(
                    f"| {FORMAT_LABELS[fmt_key]} ({variant}) | {size_mb} MB |"
                )
    file_sizes_table = f"""| Format (variant) | Size |
|--------|------|
{chr(10).join(file_sizes_rows)}"""

    # Pixel count from flat parquet (raw variant)
    flat_path = get_path("parquet_flat", "raw")
    if flat_path.exists():
        num_pixels = len(pd.read_parquet(flat_path, columns=["x"]))
    else:
        num_pixels = 0
    num_pixels_million = round(num_pixels / 1_000_000, 1) if num_pixels else 22_000 * 22_000 / 1_000_000

    # Total size of all files (all variants)
    total_size_gb = 0.0
    for fmt_key in FORMAT_LABELS:
        for variant in DATA_VARIANTS:
            p = get_path(fmt_key, variant)
            if p.exists():
                total_size_gb += p.stat().st_size
    total_size_gb /= (1024**3)

    return {
        "title": "How to Query a Massive DEM on 8GB of RAM",
        "date": today,
        "description": (
            "Benchmarking COG, Parquet+S2, Parquet+H3, and GeoParquet against "
            "DuckDB, Rasterio, Polars, and more — on a MacBook Air with 8GB RAM."
        ),
        "plots_base": "/plots",
        "num_pixels_million": num_pixels_million,
        "total_size_gb": round(total_size_gb, 1),
        "file_sizes_table": file_sizes_table,
        "sizes": sizes,
        "point_best": best.get("point", []),
        "bbox_best": best.get("bbox", []),
        "polygon_best": best.get("polygon", []),
        "num_combos": len(df.groupby(["format", "tool", "query_type"])),
        "num_runs": len(df),
        "num_successful": len(success),
    }


def read_if_exists(path: Path) -> str:
    """Read a file, returning empty string if missing."""
    if path.exists():
        return path.read_text()
    return ""


def main():
    print("=== Stage 4: Blog Post Generation ===")
    CONTENT_DIR.mkdir(parents=True, exist_ok=True)

    ctx = load_context()

    # Render template
    env = Environment(
        loader=FileSystemLoader(str(ROOT / "templates")),
        keep_trailing_newline=True,
    )
    template = env.get_template("blog_post.mdx.jinja2")
    mdx_content = template.render(**ctx)

    # Write output
    today = ctx["date"]
    output_path = CONTENT_DIR / f"{today}-dem-format-benchmark.mdx"
    output_path.write_text(mdx_content)
    print(f"Blog post written to {output_path}")
    print(f"  {len(mdx_content):,} characters")

    # Copy to dnf0.github.io if available
    blog_content = Path("/Users/danielfisher/repositories/dnf0.github.io/content/blog")
    blog_plots = Path("/Users/danielfisher/repositories/dnf0.github.io/public/plots")

    if blog_content.parent.parent.exists():
        blog_content.mkdir(parents=True, exist_ok=True)
        blog_plots.mkdir(parents=True, exist_ok=True)

        shutil.copy2(output_path, blog_content / output_path.name)
        print(f"Copied MDX to {blog_content / output_path.name}")

        for png in PLOTS_DIR.glob("*.png"):
            shutil.copy2(png, blog_plots / png.name)
        print(f"Copied {len(list(PLOTS_DIR.glob('*.png')))} plots to {blog_plots}")
    else:
        print("dnf0.github.io not found, skipping copy. Run manually:")
        print(f"  cp {output_path} /Users/danielfisher/repositories/dnf0.github.io/content/blog/")
        print(f"  cp {PLOTS_DIR}/*.png /Users/danielfisher/repositories/dnf0.github.io/public/plots/")


if __name__ == "__main__":
    main()
