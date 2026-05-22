---
name: blog-post-writing
description: Use when the user wants to write, draft, or edit an engineering blog post. Enforces strict adherence to BLOG_STYLE.md through a multi-stage approval and review pipeline.
---

# Blog Post Writing Skill

You are an expert technical writer and editor. Your goal is to guide the user through writing an engineering blog post that strictly adheres to the project's `BLOG_STYLE.md`.

**CRITICAL RULE:** You MUST follow the Progressive Refinement Pipeline below. Do NOT skip steps. Do NOT write the full draft until the outline is approved.

## Phase 1: The Input Gathering (STOP & ASK)

When this skill is invoked, you must FIRST gather all necessary context.

1. **Load Guidelines:** Explicitly read `BLOG_STYLE.md` from the repository to understand the rules.
2. **Load Reference:** Explicitly read at least one recent `.mdx` file from the `content/` directory to understand the layout in practice (e.g., `content/2026-05-12-why-vrts-fail.mdx`).
3. **Ask the User:** Prompt the user for:
   - The primary topic/goal.
   - Any raw data, benchmark outputs, or notes.
   - The key architectural takeaways.
4. **Validate:** Ensure you have enough data to populate a comprehensive "Empirical Scorecard" (metrics, latencies, etc.). If not, warn the user.

**Wait for the user's response before proceeding to Phase 2.**

## Phase 2: The Outline & Approval Gate (HARD STOP)

Once you have the inputs, generate a structured outline that maps exactly to the `BLOG_STYLE.md` structural requirements.

**Outline Structure MUST Include:**
- Working Title & Tags
- Draft TL;DR Callout (summarizing core findings)
- Introduction narrative arc
- Benchmark Environment & Hardware Constraints
- Methodology / Evaluated Architectures
- The Benchmark / Analysis (including Table/Scorecard structure)
- Conclusion: Optimal Architecture Selection

**<HARD-GATE>**
You MUST present this outline to the user and ask for their explicit approval.
Do NOT write a single paragraph of the draft until the user says "yes" or provides tweaks.
**</HARD-GATE>**

## Phase 3: Drafting & The Editorial Gauntlet

Only after outline approval, write the full draft to `content/YYYY-MM-DD-<topic>.mdx`. 
Apply the Frontmatter, British English spelling, and specific styling required by the guide.

### The Editorial Gauntlet (Self-Review Checklist)
After writing the draft, you MUST audit your own work. Do not ask the user to do this. Fix any violations you find.

Checklist:
1. **Metric Anchoring:** Does every metric (e.g., latency, size) explicitly point to a table/figure (e.g., `(see Table X)`)?
2. **Acronyms:** Are they defined on first use? (e.g., Virtual Raster (VRT)).
3. **Paragraph Flow:** Are there any single-sentence paragraphs? (Merge them).
4. **Tone:** Is the tone rigorous and objective? (Remove hyperbole like "blistering").
5. **Links:** Are all links active? (Use tools to check HTTP 200 responses).
6. **British English:** Ensure words like `optimised`, `modelling`, `centre` are used.

### Handoff
Present the final file path to the user, summarize the self-review checks you passed, and ask for their final review.