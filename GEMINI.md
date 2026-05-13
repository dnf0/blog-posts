# Blog Post Guidelines

These instructions govern the creation, formatting, and stylistic conventions for all blog posts in this repository.

## Language and Spelling
- **British English:** All blog posts, documentation, and future proposals MUST use British English spelling conventions (e.g., `optimised`, `catalogue`, `analysing`, `colour`, `centre`, `parallelisation`).

## Standardised Blog Post Structure
Every blog post must strictly adhere to the following structural order to ensure consistency across the publication:

1. **Frontmatter:** YAML frontmatter containing `title`, `date`, `tags`, and `description`.
2. **Metadata Line:** A formatted line detailing date, read time, and tags (e.g., `May 12, 2026 • 6 min read • Tags: geospatial, python, zarr`).
3. **Introduction / Problem Statement:** The opening section that defines the problem. This section MUST include a `<Callout type="info">` block containing a **Summary:** (or TL;DR) of the post's findings.
4. **Table of Contents:** An auto-generated list of links to all subsequent sections.
5. **Benchmark Environment & Hardware Constraints:** A detailed breakdown of the machine, storage, and concurrency models used during testing.
6. **Methodology / Evaluated Architectures:** A list defining the different approaches or formats being benchmarked.
7. **The Benchmark / Analysis:** The core empirical results, data tables, and technical caveats.
8. **Conclusion:** The final architectural recommendation.
9. **References:** A list setting the benchmark in context, providing links to the libraries, formats, or datasets discussed.
10. **Transparency Note:** A brief footer stating: *"Transparency Note: This blog post was written and structured with the assistance of an AI agent (Gemini)."*

## Acronyms
Acronyms that are not widely understood (e.g., STAC, COG, VRT) must be explicitly spelled out and defined upon their first use in the text. Common industry acronyms (like API, HTTP, XML, RAM, JSON) may be used without explicit definition.