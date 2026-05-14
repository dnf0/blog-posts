import re

with open("content/2026-05-10-dem-format-benchmark.mdx", "r") as f:
    text = f.read()

# Replace general references to 100,000
text = text.replace("batches of up to 100,000 irregular polygons", "batches of up to 1,000 irregular polygons")
text = text.replace("If a user queries 100,000 catchments", "If a user queries 1,000 catchments")
text = text.replace("100K+ polygons", "1K+ polygons")
text = text.replace("querying 100,000 irregular polygons", "querying 1,000 irregular polygons")
text = text.replace("Querying 100,000 global polygons", "Querying 1,000 global polygons")

# Update Rasterio numbers
text = text.replace("Extracting 100,000 polygons via Rasterio took 181 seconds.", "Extracting 1,000 polygons via Rasterio took 0.73 seconds.")

# Update Polars numbers
text = text.replace("Polars processed the database query for 100,000 polygons in 11.7 seconds.", "Polars processed the database query for 1,000 polygons in 0.18 seconds.")

# Update Rust Zarr numbers
text = text.replace("generating the Hilbert pixel masks for 100,000 polygons (the \"Covering\" step) took approximately 25-30 seconds", "generating the Hilbert pixel masks for 1,000 polygons (the \"Covering\" step) took approximately 0.27 seconds")
text = text.replace("This pipeline extracted 100,000 polygons in 1.57 seconds.", "This pipeline extracted 1,000 polygons in 0.24 seconds.")
text = text.replace("processing 100,000 polygons takes 10 times longer than processing 10,000 polygons", "processing 1,000 polygons takes 10 times longer than processing 100 polygons")
text = text.replace("This 1.57-second execution was run locally.", "This 0.24-second execution was run locally.")

# Update Lance comparison
text = text.replace("(~65 seconds for 100K polygons, compared to Zarr's 1.5 seconds)", "(~0.46 seconds for 1K polygons, compared to Zarr's 0.24 seconds)")

# Update caching paragraphs
text = text.replace("within a single 100,000-polygon batch", "within a massive batch")
text = text.replace("querying 100,000 localized polygons", "querying thousands of localized polygons")
text = text.replace("querying 100,000 disparate polygons", "querying thousands of disparate polygons globally")

with open("content/2026-05-10-dem-format-benchmark.mdx", "w") as f:
    f.write(text)
