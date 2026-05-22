# Blog Post Writing Skill Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Create a Gemini CLI skill that guides users through a Progressive Refinement Pipeline for writing engineering blog posts that strictly conform to `BLOG_STYLE.md`.

**Architecture:** The skill is a structured markdown document (`SKILL.md`) containing workflow instructions for the AI. It guides the AI to halt for approval after outlining, and forces a rigorous self-review after drafting.

**Tech Stack:** Markdown (Prompt Engineering)

---

### Task 1: Initialize the Skill File and Frontmatter

**Files:**
- Create: `skills/blog-post-writing/SKILL.md`

- [ ] **Step 1: Create the directory**

```bash
mkdir -p skills/blog-post-writing
```

- [ ] **Step 2: Write the Frontmatter and Overview**

```bash
cat << 'EOF' > skills/blog-post-writing/SKILL.md
---
name: blog-post-writing
description: Use when the user wants to write, draft, or edit an engineering blog post. Enforces strict adherence to BLOG_STYLE.md through a multi-stage approval and review pipeline.
---

# Blog Post Writing Skill

You are an expert technical writer and editor. Your goal is to guide the user through writing an engineering blog post that strictly adheres to the project's `BLOG_STYLE.md`.

**CRITICAL RULE:** You MUST follow the Progressive Refinement Pipeline below. Do NOT skip steps. Do NOT write the full draft until the outline is approved.

EOF
```

- [ ] **Step 3: Verify file creation**

Run: `cat skills/blog-post-writing/SKILL.md`
Expected: Output shows the frontmatter and overview.

- [ ] **Step 4: Commit**

```bash
git add skills/blog-post-writing/SKILL.md
git commit -m "feat: initialize blog-post-writing skill with frontmatter"
```

---

### Task 2: Implement Phase 1 (Trigger & Input Phase)

**Files:**
- Modify: `skills/blog-post-writing/SKILL.md`

- [ ] **Step 1: Append Phase 1 instructions**

```bash
cat << 'EOF' >> skills/blog-post-writing/SKILL.md
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
EOF
```

- [ ] **Step 2: Verify addition**

Run: `grep "Phase 1: The Input Gathering" skills/blog-post-writing/SKILL.md`
Expected: Match found.

- [ ] **Step 3: Commit**

```bash
git add skills/blog-post-writing/SKILL.md
git commit -m "feat(skill): add Phase 1 input gathering instructions"
```

---

### Task 3: Implement Phase 2 (Outline & Approval Gate)

**Files:**
- Modify: `skills/blog-post-writing/SKILL.md`

- [ ] **Step 1: Append Phase 2 instructions**

```bash
cat << 'EOF' >> skills/blog-post-writing/SKILL.md

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
EOF
```

- [ ] **Step 2: Verify addition**

Run: `grep "HARD-GATE" skills/blog-post-writing/SKILL.md`
Expected: Match found.

- [ ] **Step 3: Commit**

```bash
git add skills/blog-post-writing/SKILL.md
git commit -m "feat(skill): add Phase 2 outline and hard gate instructions"
```

---

### Task 4: Implement Phase 3 (Drafting & Editorial Gauntlet)

**Files:**
- Modify: `skills/blog-post-writing/SKILL.md`

- [ ] **Step 1: Append Phase 3 instructions**

```bash
cat << 'EOF' >> skills/blog-post-writing/SKILL.md

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
EOF
```

- [ ] **Step 2: Verify addition**

Run: `grep "Editorial Gauntlet" skills/blog-post-writing/SKILL.md`
Expected: Match found.

- [ ] **Step 3: Commit**

```bash
git add skills/blog-post-writing/SKILL.md
git commit -m "feat(skill): add Phase 3 drafting and editorial gauntlet"
```
