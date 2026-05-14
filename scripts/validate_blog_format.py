#!/usr/bin/env python3
import sys
import os
import glob
import re

def validate_file(filepath):
    with open(filepath, 'r') as f:
        content = f.read()

    errors = []

    # 1. Check for redundant H1 right after frontmatter
    # Frontmatter ends with `---`. If the first non-empty line after is `# `, that's an error.
    parts = content.split("---")
    if len(parts) >= 3:
        body = "---".join(parts[2:]).strip()
        first_line = body.split("\n")[0].strip()
        if first_line.startswith("# "):
            errors.append(f"Redundant H1 title found in markdown body: '{first_line}'. Next.js renders the frontmatter title automatically.")

    # 2. Check for hardcoded date in metadata line
    # Look for patterns like *May 10, 2026 • 
    if re.search(r"\*[A-Z][a-z]{2,8} \d{1,2}, \d{4} •", content):
        errors.append("Hardcoded date found in the metadata line. Next.js extracts the date from the frontmatter automatically. Format should be: *X min read • Tags: ...*")

    if errors:
        print(f"\n❌ Validation failed for {filepath}:")
        for err in errors:
            print(f"  - {err}")
        return False
    
    return True

def main():
    mdx_files = glob.glob("**/*.mdx", recursive=True)
    all_passed = True

    for f in mdx_files:
        if "node_modules" in f or ".next" in f:
            continue
        if not os.path.exists(f):
            continue
        if not validate_file(f):
            all_passed = False

    if not all_passed:
        print("\nFix the formatting errors above to comply with BLOG_STYLE.md before committing.")
        sys.exit(1)
    
    print("✅ All blog posts passed formatting validation.")
    sys.exit(0)

if __name__ == "__main__":
    main()
