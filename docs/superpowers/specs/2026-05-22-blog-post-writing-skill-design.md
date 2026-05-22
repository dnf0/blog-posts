# Blog Post Writing Skill Design

## Overview
A new Gemini CLI skill designed to assist in writing engineering blog posts that strictly adhere to the project's `BLOG_STYLE.md`. It uses a Progressive Refinement Pipeline, guiding the user from data gathering through outlining, drafting, and finally, a rigorous self-review process.

## Architecture & Workflow

### 1. The Skill Trigger & Input Phase
**Goal:** Gather required context and validate inputs before any writing occurs.
- **Action:** The skill prompts the user for core inputs: topic, benchmark results, raw data files, and key takeaways.
- **Validation:** 
  - Loads `BLOG_STYLE.md` and reference posts (e.g., the VRT post) into context.
  - Verifies the provided data is sufficient to meet empirical requirements (e.g., ensuring metrics exist for the required scorecard).

### 2. The Outline & Approval Gate
**Goal:** Establish structural correctness according to `BLOG_STYLE.md` before investing in heavy prose generation.
- **Action:** Generates a structured outline mapping to required sections:
  - Working Title & Tags
  - Draft TL;DR Callout
  - Introduction narrative arc
  - Benchmark Environment setup
  - Evaluated Architectures list
  - Table/Scorecard structure
  - Conclusion recommendation
- **Hard Stop:** The skill halts execution and demands explicit user approval or requested tweaks before proceeding to the drafting phase.

### 3. Drafting & The Editorial Gauntlet
**Goal:** Produce the final `.mdx` content and enforce strict editorial standards.
- **Drafting:** Writes the full draft to `content/YYYY-MM-DD-<topic>.mdx`, applying required Frontmatter, British English spelling, academic tone, and specific markdown formatting.
- **Self-Review Checklist:** The skill audits its own draft against `BLOG_STYLE.md`:
  - Metric anchoring (e.g., verifying `(see Table X)` follows data points).
  - Acronym definitions on first use.
  - Elimination of single-sentence paragraphs.
  - Hyperbole check (ensuring an objective, measured tone).
  - Link verification (using network tools to ensure HTTP 200 responses for references).
- **Refinement & Handoff:** Fixes identified violations, then presents the final file path to the user along with a summary of the passed checks.

## File Locations
- Spec: `docs/superpowers/specs/2026-05-22-blog-post-writing-skill-design.md`
- Skill (Future): `.gemini/extensions/superpowers/skills/blog-post-writing/SKILL.md` (or similar standard skill location).