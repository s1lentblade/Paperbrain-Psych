#!/usr/bin/env python3
"""
patch_topics_full.py — Two things at once:

1. Update paper note tags from subfield slug → primary topic slug,
   so each paper matches its topic's color group in graph.json.

2. Expand the ## Topic section to list ALL topics from paper_topics table
   (not just the primary one), creating a true second-brain web.

3. Update graph.json: replace subfield-level paper color groups with
   per-topic color groups (matching the existing tag:topic entries).

Usage:
    python scripts/patch_topics_full.py
"""

import json
import re
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR  = Path(__file__).parent
DB_PATH     = SCRIPT_DIR.parent / "data" / "papers.db"
VAULT_PATH  = SCRIPT_DIR.parent / "full psychology breakdown"
PAPERS_DIR  = VAULT_PATH / "Papers"
GRAPH_JSON  = VAULT_PATH / ".obsidian" / "graph.json"


def slugify(s: str) -> str:
    s = s.lower().strip()
    s = re.sub(r"[^a-z0-9\s-]", "", s)
    s = re.sub(r"\s+", "-", s)
    s = re.sub(r"-+", "-", s).strip("-")
    return s


# ---------------------------------------------------------------------------
# Step 1 — Update graph.json
# ---------------------------------------------------------------------------

def update_graph_json() -> int:
    cfg = json.loads(GRAPH_JSON.read_text(encoding="utf-8"))

    # Collect existing tag:topic slug → color mappings
    topic_colors: dict[str, dict] = {}
    for g in cfg["colorGroups"]:
        m = re.match(r"^tag:topic tag:(.+)$", g["query"].strip())
        if m:
            topic_colors[m.group(1)] = g["color"]

    # Remove all existing tag:paper entries (subfield-level ones)
    cfg["colorGroups"] = [
        g for g in cfg["colorGroups"]
        if not re.match(r"tag:paper\b", g["query"].strip())
    ]

    # Add per-topic paper color groups (same RGB, alpha 0.75 = slightly dimmer)
    paper_groups = [
        {
            "query": f"tag:paper tag:{slug}",
            "color": {"a": 0.75, "rgb": color["rgb"]},
        }
        for slug, color in topic_colors.items()
    ]

    cfg["colorGroups"] = paper_groups + cfg["colorGroups"]
    GRAPH_JSON.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    return len(paper_groups)


# ---------------------------------------------------------------------------
# Step 2 — Patch paper notes
# ---------------------------------------------------------------------------

def patch_notes() -> None:
    files = list(PAPERS_DIR.glob("*.md"))
    print(f"  {len(files):,} notes to process", flush=True)

    # Collect openalex IDs
    oa_map: dict[str, Path] = {}
    for f in files:
        try:
            text = f.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r'^openalex_id: "(.+?)"', text, re.MULTILINE)
            if m:
                oa_map[m.group(1)] = f
        except Exception:
            pass

    print(f"  {len(oa_map):,} notes have openalex_id", flush=True)

    # Bulk-fetch ALL topics per paper from paper_topics table
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row

    all_ids = list(oa_map.keys())
    topics_by_paper: dict[str, list[tuple[str, int]]] = {}
    chunk = 900
    for start in range(0, len(all_ids), chunk):
        piece = all_ids[start : start + chunk]
        ph = ",".join("?" * len(piece))
        rows = conn.execute(
            f"""
            SELECT paper_id, topic_name, is_primary
            FROM paper_topics
            WHERE paper_id IN ({ph})
            ORDER BY paper_id, is_primary DESC, topic_name
            """,
            piece,
        ).fetchall()
        for r in rows:
            topics_by_paper.setdefault(r["paper_id"], []).append(
                (r["topic_name"], r["is_primary"])
            )
    conn.close()
    print(f"  Loaded topics for {len(topics_by_paper):,} papers", flush=True)

    # Rewrite notes
    updated = skipped = errors = 0
    total = len(files)

    for idx, fpath in enumerate(files, 1):
        try:
            content = fpath.read_text(encoding="utf-8", errors="ignore")

            # Extract frontmatter fields
            topic_m    = re.search(r'^topic: "(.+?)"',    content, re.MULTILINE)
            subfield_m = re.search(r'^subfield: "(.+?)"', content, re.MULTILINE)
            oa_m       = re.search(r'^openalex_id: "(.+?)"', content, re.MULTILINE)

            if not topic_m:
                skipped += 1
                continue

            primary_topic = topic_m.group(1)
            subfield      = subfield_m.group(1) if subfield_m else ""
            oa_id         = oa_m.group(1) if oa_m else ""
            topic_slug    = slugify(primary_topic)

            new = content

            # ── 1. Fix tags line ──────────────────────────────────────────
            new = re.sub(
                r"^tags: \[paper(?:, [^\]]*)?\]",
                f"tags: [paper, {topic_slug}]",
                new,
                flags=re.MULTILINE,
            )

            # ── 2. Rebuild Topic section ──────────────────────────────────
            all_topics = topics_by_paper.get(oa_id, [])

            if all_topics:
                lines = []
                for topic_name, is_primary in all_topics:
                    marker = " *(primary)*" if is_primary else ""
                    lines.append(f"- [[{topic_name}]]{marker}")
                if subfield:
                    lines.append(f"- [[{subfield}]]")
                topic_block = "## Topics\n\n" + "\n".join(lines) + "\n"
            else:
                # fallback: at least keep primary + subfield
                lines = [f"- [[{primary_topic}]] *(primary)*"]
                if subfield:
                    lines.append(f"- [[{subfield}]]")
                topic_block = "## Topics\n\n" + "\n".join(lines) + "\n"

            # Replace existing ## Topic[s] section (to end of file or next ##)
            new = re.sub(
                r"## Topics?\n\n[\s\S]*?(?=\n## |\Z)",
                topic_block,
                new,
            )

            if new != content:
                fpath.write_text(new, encoding="utf-8")
                updated += 1
            else:
                skipped += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"\n  WARN {fpath.name}: {e}", flush=True)

        if idx % 1000 == 0 or idx == total:
            print(
                f"  {idx:,}/{total:,}  updated={updated:,}  skipped={skipped:,}",
                end="\r",
                flush=True,
            )

    print(f"\n  Done.  updated={updated:,}  skipped={skipped:,}  errors={errors}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not DB_PATH.exists():
        sys.exit(f"ERROR: DB not found at {DB_PATH}")

    print("Step 1 — Updating graph.json…", flush=True)
    n = update_graph_json()
    print(f"  Added {n} per-topic paper color groups", flush=True)

    print("Step 2 — Patching paper notes…", flush=True)
    patch_notes()

    print("\nAll done. Reload Obsidian and toggle Brain View to see the result.")


if __name__ == "__main__":
    main()
