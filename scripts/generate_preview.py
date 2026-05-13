import markdown_it
from pathlib import Path

md = markdown_it.MarkdownIt()
content = Path("content/2026-05-12-why-vrts-fail.mdx").read_text()

html = md.render(content)

full_html = f"""
<!DOCTYPE html>
<html>
<head>
<title>Preview: Why Global VRTs Fail</title>
<style>
  body {{ max-width: 900px; margin: 40px auto; padding: 20px; font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; line-height: 1.6; color: #333; }}
  pre {{ background: #f6f8fa; padding: 16px; overflow-x: auto; border-radius: 6px; font-size: 85%; }}
  code {{ font-family: ui-monospace, SFMono-Regular, "SF Mono", Menlo, Consolas, "Liberation Mono", monospace; }}
  img {{ max-width: 100%; height: auto; border-radius: 8px; box-shadow: 0 4px 6px rgba(0,0,0,0.1); margin-top: 20px; }}
  h1, h2, h3 {{ border-bottom: 1px solid #eaecef; padding-bottom: 0.3em; }}
  .callout {{ padding: 15px; margin: 20px 0; border: 1px solid #eee; border-left-width: 5px; border-radius: 3px; border-left-color: #0366d6; background-color: #f1f8ff; }}
</style>
</head>
<body>
{html.replace('<Callout type="info">', '<div class="callout">').replace('</Callout>', '</div>')}
</body>
</html>
"""

Path("content/preview.html").write_text(full_html)
print("Preview HTML generated at content/preview.html")
