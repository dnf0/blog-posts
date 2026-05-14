import re

with open("content/2026-05-10-dem-format-benchmark.mdx", "r") as f:
    text = f.read()

part1 = text[:text.find("## Scaling Back to the Globe")]
part2 = text[text.find("## Scaling Back to the Globe"):text.find("### Conclusion")]
conclusion = text[text.find("### Conclusion"):text.find("## Future Outlook: Lance and Icechunk")]
lance_section = text[text.find("## Future Outlook: Lance and Icechunk"):text.find("2. **Icechunk:**")]
icechunk = text[text.find("2. **Icechunk:**"):text.find("## Reproduce This")]
rest = text[text.find("## Reproduce This"):]

# Modify Lance Section
lance_section = lance_section.replace(
    "## Future Outlook: Lance and Icechunk\n\nWhile Zarr v3 and Parquet are established formats, emerging technologies are actively addressing spatial data limitations:\n\n1. **Lance Format:** Lance [12] is",
    "## Spatial Chunking vs. Row-Level Random Access\n\nWhile Zarr v3 and Parquet are established formats, emerging technologies like Lance [12] are actively addressing spatial data limitations. Lance is"
)
lance_section = lance_section.replace("### Spatial Chunking vs. Row-Level Random Access\n", "")

# Modify Icechunk Section
icechunk = "### Transactional Data Cubes: Icechunk\n" + icechunk.replace("2. **Icechunk:** ", "")

# Assemble
new_text = part1 + lance_section + "\n" + part2 + icechunk + "\n## Conclusion\n" + conclusion.replace("### Conclusion\n", "") + "\n" + rest

# Cleanup extra newlines
new_text = re.sub(r'\n{3,}', '\n\n', new_text)

with open("content/2026-05-10-dem-format-benchmark.mdx", "w") as f:
    f.write(new_text)
