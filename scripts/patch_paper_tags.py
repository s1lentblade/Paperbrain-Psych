#!/usr/bin/env python3
"""
patch_paper_tags.py — Add slugified subfield tag to existing paper notes
so graph.json color groups can match papers to their topic cluster.

Usage:  python scripts/patch_paper_tags.py
"""

import re
from pathlib import Path

PAPERS_DIR = (
    Path(__file__).parent.parent
    / "full psychology breakdown"
    / "Papers"
)


def slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


def main() -> None:
    files = list(PAPERS_DIR.glob("*.md"))
    print(f"Patching {len(files):,} notes…", flush=True)

    updated = skipped = errors = 0

    for i, fpath in enumerate(files, 1):
        try:
            content = fpath.read_text(encoding="utf-8")

            # Already patched?
            if re.search(r"^tags: \[paper, ", content, re.MULTILINE):
                skipped += 1
                continue

            # Extract subfield from YAML frontmatter
            m = re.search(r'^subfield: "(.+?)"', content, re.MULTILINE)
            if not m:
                skipped += 1
                continue

            slug = slugify(m.group(1))
            if not slug:
                skipped += 1
                continue

            new_content = re.sub(
                r"^tags: \[paper\]",
                f"tags: [paper, {slug}]",
                content,
                flags=re.MULTILINE,
            )

            if new_content == content:
                skipped += 1
                continue

            fpath.write_text(new_content, encoding="utf-8")
            updated += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"\n  WARN {fpath.name}: {e}", flush=True)

        if i % 1000 == 0 or i == len(files):
            print(f"  {i:,}/{len(files):,}  updated={updated:,}  skipped={skipped:,}",
                  end="\r", flush=True)

    print(f"\nDone.  updated={updated:,}  skipped={skipped:,}  errors={errors}")


if __name__ == "__main__":
    main()
