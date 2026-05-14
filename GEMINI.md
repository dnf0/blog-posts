# Blog Post Guidelines

These instructions govern the creation, formatting, and stylistic conventions for all blog posts in this repository. 

**IMPORTANT: You MUST also strictly adhere to the rigorous editorial and stylistic standards defined in the top-level [`BLOG_STYLE.md`](./BLOG_STYLE.md) document.**

## Language and Spelling
- **British English:** All blog posts, documentation, and future proposals MUST use British English spelling conventions (e.g., `optimised`, `catalogue`, `analysing`, `colour`, `centre`, `parallelisation`).

## Standardised Blog Post Structure
Every blog post must strictly adhere to the following structural order to ensure consistency across the publication:

1. **Frontmatter:** YAML frontmatter containing `title`, `date`, `tags`, and `description`.
2. **Metadata Line:** An italicised line detailing read time and code-formatted tags (e.g., `*6 min read • Tags: \`geospatial\`, \`python\`*`).
3. **TL;DR Callout:** A `<Callout type="info">` block starting with `**TL;DR:**` summarising the findings.
4. **Introduction / Problem Statement:** The opening narrative paragraphs (without an H2 header).
5. **Table of Contents:** An auto-generated list of links to all subsequent sections.
6. **Benchmark Environment & Hardware Constraints:** A detailed breakdown of the machine, storage, and concurrency models used during testing.
7. **Methodology / Evaluated Architectures:** A list defining the different approaches or formats being benchmarked.
8. **The Benchmark / Analysis:** The core empirical results, data tables, and technical caveats.
9. **Conclusion:** The final architectural recommendation.
10. **References:** A list setting the benchmark in context, providing links to the libraries, formats, or datasets discussed.
11. **Transparency Note:** A brief footer stating: *"Transparency Note: This blog post was written and structured with the assistance of an AI agent (Gemini)."*

## Acronyms
Acronyms that are not widely understood (e.g., STAC, COG, VRT) must be explicitly spelled out and defined upon their first use in the text. Common industry acronyms (like API, HTTP, XML, RAM, JSON) may be used without explicit definition.