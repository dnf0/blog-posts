# Engineering Blog Style Guide

This document defines the strict editorial, stylistic, and architectural standards for all engineering blog posts in this repository. AI agents must ingest and adhere to these rules before drafting or editing content.

## 1. Tone and Voice
*   **Academic & Professional:** Maintain a rigorous, objective, and measured tone.
*   **British English:** Strictly enforce British English spelling (e.g., "optimised", "modelling", "centre"). Note: "perimeter" remains "perimeter".
*   **First-Person Singular:** Use "I" and "my" to frame the narrative around personal engineering experience and empirical discovery.
*   **No Hyperbole:** Eradicate all marketing fluff and hyperbolic buzzwords ("blistering", "lightning-fast", "severe", "revolutionary"). Use precise, grounded adjectives ("substantial", "optimal", "microscopic constant factor").
*   **Direct Vocabulary:** Prefer simple, direct verbs. Always use "use" instead of "utilise".

## 2. Structural Flow
*   **Cohesive Paragraphs:** Absolutely no single-sentence paragraphs. Merge related thoughts into robust, flowing blocks of text. The rhythm should resemble a dense engineering paper, not a slide deck.
*   **Chronological Logic:** Ensure concepts are defined *before* they are cited as bottlenecks. (e.g., Explain what the "Covering Step" is before discussing its performance penalty).
*   **Summary Matrices:** Include a comprehensive Markdown table (an "Empirical Scorecard") right before the Conclusion to synthesize complex metrics (Storage Size, Latency, Big-O Scaling, etc.) into an at-a-glance format.

## 3. Empirical Rigour & Anchoring
*   **Anchor Every Metric:** Every claim, file size (e.g., "1.7 MB"), and latency metric (e.g., "0.24 seconds") MUST be explicitly anchored to a visual reference. Always append `(see Figure X)` or `(see Table Y)` immediately after the data point.
*   **Exact Dimensionality:** Be precise about scale. If a dataset has *over* 700 layers, say "over 700 distinct layers," not exactly 700.
*   **Avoid Theoretical Absolutes:** If a behavior is a theoretical consequence of an architecture rather than an empirically measured fact in the benchmark, explicitly state that it "would theoretically" behave that way.

## 4. Explaining Jargon & Concepts
*   **Unpack Domain Knowledge:** Do not assume the reader possesses specialized domain knowledge (e.g., GIS heuristics like "Centre-Point" vs. "All-Touched"). Explain the *mechanics* of how a heuristic or algorithm works so any software engineer can understand it.
*   **Explicit Definitions:** Clearly define architectural paradigms (e.g., "Spatial Locality" vs. "Row-Level Retrieval") before comparing them.
*   **Educational Hyperlinks:** Add Markdown hyperlinks to Wikipedia or foundational resources for core computer science concepts (e.g., Quadtree Compaction, Run-Length Encoding) on their first mention.
*   **Acronym Rules:** Define acronyms exactly once on their first usage (e.g., Time-To-First-Byte (TTFB)). Use the raw acronym for all subsequent mentions.

## 5. Production & Pragmatic Context
*   **Ground in Cloud Reality:** Local benchmarks are insufficient. Always extrapolate local results to cloud production realities. Explicitly discuss network bottlenecks like S3 Time-To-First-Byte (TTFB) latencies, sequential header fetching, and filesystem inode limits.
*   **Explain Caching Artifacts:** Acknowledge when a benchmark is effectively a "hot cache" or "cold start" test, and explain how OS page caching or RAM indexing impacts the results.
*   **Organizational Pragmatism:** Architecture is built by humans. Explicitly acknowledge the organizational and operational trade-offs of a solution (e.g., the CI/CD and debugging friction of introducing Rust into a Python stack). Validate when an approach is a "write-once" operational cost versus continuous technical debt.

## 6. Graphical Style & Aesthetics
*   **Modern Tech Aesthetic:** Use styling inspired by leading data engineering and geospatial tech blogs (e.g., Stripe, Vercel, Mapbox).
*   **Typography:** Prefer clean, modern sans-serif fonts (e.g., Inter, system-ui) rather than standard browser defaults. Use left-aligned text (ragged right) with generous line-heights (1.6 to 1.8) for maximum readability, mobile responsiveness, and web accessibility. Avoid justified text.
*   **Color Palette:** Adopt a sophisticated Slate/Zinc color palette. Use dark gray/slate for text rather than pure black, and subtle light grays for backgrounds and borders.
*   **Code Blocks & Inline Code:** Code blocks should have a distinct dark background (e.g., `#1e293b`) with light text, rounded corners, and padding. Inline code should use a subtle gray background with dark text to stand out from prose without being visually aggressive.
*   **Tables & Blockquotes:** Tables should be styled as clean, modern cards with subtle borders, padding, and a distinguished header row. Blockquotes should feature a colored left-border and italicized text to clearly offset them as callouts.