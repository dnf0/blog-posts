# Engineering Blog Style Guide

This document defines the strict editorial, stylistic, and architectural standards for all engineering blog posts in this repository. AI agents must ingest and adhere to these rules before drafting or editing content.

## 1. Post Types: Analytical vs. Conversational
Blog posts in this repository fall into one of two distinct structural types. The writer must explicitly determine the post type before drafting:

### Type A: Analytical (Heavy-Duty Engineering & Benchmarks)
Used for deep technical dives, format comparisons, and performance benchmarking.
- **Tone:** Academic, rigorous, objective, and measured.
- **Structure:** MUST include a "Benchmark Environment" section, "Methodology", and an "Empirical Scorecard" matrix synthesizing complex metrics (Storage Size, Latency, Big-O Scaling, etc.).
- **Empirical Rigour:** Every metric (e.g., "1.7 MB", "0.24s") MUST be explicitly anchored to a visual reference `(see Table Y)`.

### Type B: Conversational (Tooling, Workflow, & Opinions)
Used for shorter, practical walkthroughs, CI/CD tips, or opinionated pieces where deep empirical matrices are overkill.
- **Tone:** Conversational, engaging, and slightly lighter, but still professional. Analogies are welcome.
- **Structure:** Flexible headings. Does NOT require an "Empirical Scorecard" or a strict "Methodology" section. 
- **Acronyms:** Do not define widely known acronyms (e.g., AI, LLM, API, SVG, JSON) as this breaks the conversational flow. Define only niche domain terms.

## 2. Universal Standards (Applies to All Types)

### Standardised Post Layout
1. **Metadata Line:** An italicised line containing the read time and code-formatted tags (e.g., `*18 min read • Tags: \`geospatial\`, \`python\`*`).
2. **TL;DR Callout:** A `<Callout type="info">` block starting with `**TL;DR:**` containing the core takeaway. This must immediately follow the metadata line.
3. **Introduction:** The opening narrative paragraphs flowing directly after the callout, without a preceding H2 header.

### Tone and Voice
*   **British English:** Strictly enforce British English spelling (e.g., "optimised", "modelling", "centre"). Note: "perimeter" remains "perimeter".
*   **First-Person Singular:** Use "I" and "my" (or "we" for team efforts) to frame the narrative around personal engineering experience.
*   **No Hyperbole:** Eradicate all marketing fluff and hyperbolic buzzwords ("blistering", "lightning-fast", "revolutionary"). Use precise adjectives.
*   **Direct Vocabulary:** Prefer simple, direct verbs. Always use "use" instead of "utilise".

### Structural Flow
*   **Cohesive Paragraphs:** Absolutely no single-sentence paragraphs. Merge related thoughts into robust, flowing blocks of text. 

### References & Anchoring
*   **Inline Academic Referencing:** References MUST be cited inline throughout the text using bracketed numbers (e.g., `[1]`). Do not list references at the bottom without explicitly citing them in the body of the text where the concept is introduced.
*   **Active Link Verification:** Before finalizing or publishing a post, you MUST explicitly verify that all URLs in the References section are active and return a successful HTTP status (e.g., 200 OK).

## 3. Explaining Jargon & Concepts
*   **Unpack Domain Knowledge:** Explain the *mechanics* of how a heuristic or algorithm works so any software engineer can understand it.
*   **Educational Hyperlinks:** Add Markdown hyperlinks to Wikipedia or foundational resources for core computer science concepts on their first mention.

## 4. Graphical Style & Aesthetics
*   **Modern Tech Aesthetic:** Use styling inspired by leading data engineering blogs.
*   **Code Blocks & Inline Code:** Code blocks should have a distinct dark background with light text. Inline code should use a subtle gray background with dark text to stand out from prose without being visually aggressive.