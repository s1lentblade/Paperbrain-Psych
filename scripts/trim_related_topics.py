#!/usr/bin/env python3
"""
trim_related_topics.py

Strips cross-subfield links from every psychology topic note's
## Related topics section, keeping only same-subfield co-occurrence links.
Also tunes graph.json physics for a cleaner topic map layout.
"""

import json
import re
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
VAULT_PATH = SCRIPT_DIR.parent / "full psychology breakdown"
TOPICS_DIR = VAULT_PATH / "Topics"
GRAPH_JSON = VAULT_PATH / ".obsidian" / "graph.json"


def main() -> None:
    # ── Build topic_name -> subfield mapping from frontmatter ─────────────────
    print("Building topic -> subfield map...")
    topic_subfield: dict[str, str] = {}
    psych_topic_files: list[Path] = []

    for f in TOPICS_DIR.glob("*.md"):
        try:
            txt = f.read_text(encoding="utf-8", errors="ignore")
            sf_m   = re.search(r'^subfield: "(.+?)"', txt, re.MULTILINE)
            type_m = re.search(r'^type: "(.+?)"',     txt, re.MULTILINE)
            if sf_m and type_m and type_m.group(1) == "topic":
                subfield = sf_m.group(1)
                topic_subfield[f.stem] = subfield
                psych_topic_files.append(f)
        except Exception:
            pass

    print(f"  {len(topic_subfield)} psychology topic notes found")

    # ── Patch each topic note ─────────────────────────────────────────────────
    print("Trimming cross-subfield links...")
    updated = skipped = removed_total = 0

    for fpath in psych_topic_files:
        try:
            content = fpath.read_text(encoding="utf-8", errors="ignore")
            my_subfield = topic_subfield.get(fpath.stem, "")

            # Find ## Related topics section
            section_m = re.search(
                r'(## Related topics\n\n)([\s\S]*?)(?=\n## |\Z)',
                content,
            )
            if not section_m:
                skipped += 1
                continue

            header   = section_m.group(1)
            raw_body = section_m.group(2)

            # Filter lines: keep only links to same-subfield topics
            kept_lines = []
            removed = 0
            for line in raw_body.splitlines():
                wl_m = re.search(r'\[\[(.+?)\]\]', line)
                if not wl_m:
                    # Non-link lines (blank, etc.) keep as-is
                    kept_lines.append(line)
                    continue
                linked_topic = wl_m.group(1)
                linked_sf    = topic_subfield.get(linked_topic, "")
                if linked_sf == my_subfield:
                    kept_lines.append(line)
                else:
                    removed += 1

            if removed == 0:
                skipped += 1
                continue

            removed_total += removed
            new_body    = "\n".join(kept_lines)
            new_section = header + new_body
            new_content = content[:section_m.start()] + new_section + content[section_m.end():]

            fpath.write_text(new_content, encoding="utf-8")
            updated += 1

        except Exception as e:
            print(f"  WARN {fpath.name}: {e}")

    print(f"  Updated: {updated}  |  Skipped (no cross-links): {skipped}")
    print(f"  Cross-subfield links removed: {removed_total:,}")

    # ── Tune graph.json physics ───────────────────────────────────────────────
    print("Updating graph.json physics...")
    cfg = json.loads(GRAPH_JSON.read_text(encoding="utf-8"))
    cfg["lineSizeMultiplier"] = 0.5
    cfg["repelStrength"]      = 58
    cfg["centerStrength"]     = 0
    cfg["linkDistance"]       = 50
    GRAPH_JSON.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    print("  lineSizeMultiplier=0.5  repelStrength=58  centerStrength=0")

    print("\nDone. Reload Obsidian.")


if __name__ == "__main__":
    main()
