#!/usr/bin/env python3
"""
restore_multitopic.py

Makes full-psych look like the psychology vault:

Step 1 — Restore multi-topic wikilinks in every paper note.
          Each paper links to ALL its psychology topics (not just primary).
          Cross-disciplinary topics are excluded — they have no hierarchy.

Step 2 — Switch paper tags from topic-slug to subfield-slug.
          Matches the psychology vault coloring scheme (7 colors per subfield).

Step 3 — Update graph.json:
          Replace 144 per-topic paper colorGroups with 7 per-subfield colorGroups,
          matching the exact colors used in the psychology vault.
"""

import json
import re
import sqlite3
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR.parent / "data" / "papers.db"
VAULT_PATH = SCRIPT_DIR.parent / "full psychology breakdown"
PAPERS_DIR = VAULT_PATH / "Papers"
GRAPH_JSON = VAULT_PATH / ".obsidian" / "graph.json"

# Subfield colors — paper nodes match their cluster accent
# Taken from the psychology vault's graph.json paper colorGroups
SUBFIELD_PAPER_COLORS = {
    "Experimental and Cognitive Psychology": {"a": 1, "rgb": 11702484},
    "Clinical Psychology":                   {"a": 1, "rgb": 9482964},
    "Social Psychology":                     {"a": 1, "rgb": 13930640},
    "Applied Psychology":                    {"a": 1, "rgb": 9491657},
    "Developmental and Educational Psychology": {"a": 1, "rgb": 9491600},
    "General Psychology":                    {"a": 1, "rgb": 13945744},
    "Neuropsychology and Physiological Psychology": {"a": 1, "rgb": 13939344},
}


def slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '-', s)
    return s.strip('-')


# ---------------------------------------------------------------------------
# Step 1 + 2 — Patch paper notes
# ---------------------------------------------------------------------------

def patch_papers(conn: sqlite3.Connection, valid_topics: set[str]) -> None:
    files = list(PAPERS_DIR.glob("*.md"))
    total = len(files)
    print(f"  {total:,} paper notes to process")

    # Bulk-load paper_topics for all openalex_ids in the vault
    print("  Loading paper_topics from DB...", flush=True)
    id_to_file: dict[str, Path] = {}
    for f in files:
        try:
            txt = f.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r'^openalex_id: "(.+?)"', txt, re.MULTILINE)
            if m:
                id_to_file[m.group(1)] = f
        except Exception:
            pass

    all_ids = list(id_to_file.keys())
    topics_by_paper: dict[str, list[str]] = {}   # oa_id -> [topic_name, ...]
    chunk = 900
    for i in range(0, len(all_ids), chunk):
        piece = all_ids[i:i + chunk]
        ph = ",".join("?" * len(piece))
        rows = conn.execute(
            f"""
            SELECT paper_id, topic_name, is_primary
            FROM paper_topics
            WHERE paper_id IN ({ph})
              AND topic_name IN ({','.join('?' * len(valid_topics))})
            ORDER BY paper_id, is_primary DESC, topic_name
            """,
            piece + list(valid_topics),
        ).fetchall()
        for r in rows:
            topics_by_paper.setdefault(r["paper_id"], []).append(r["topic_name"])

    print(f"  {len(topics_by_paper):,} papers have topic data", flush=True)

    # Patch each file
    updated = skipped = errors = 0
    for idx, fpath in enumerate(files, 1):
        try:
            content = fpath.read_text(encoding="utf-8", errors="ignore")

            topic_m    = re.search(r'^topic: "(.+?)"',    content, re.MULTILINE)
            subfield_m = re.search(r'^subfield: "(.+?)"', content, re.MULTILINE)
            oa_m       = re.search(r'^openalex_id: "(.+?)"', content, re.MULTILINE)
            tags_m     = re.search(r'^tags: \[(.+?)\]',   content, re.MULTILINE)

            if not topic_m or not subfield_m:
                skipped += 1
                continue

            primary  = topic_m.group(1)
            subfield = subfield_m.group(1)
            oa_id    = oa_m.group(1) if oa_m else ""

            # ── Tags: subfield slug (not topic slug) ───────────────────────
            sf_slug = slugify(subfield)
            if tags_m:
                raw_tags = tags_m.group(1)
                tag_list = [t.strip().strip('"') for t in raw_tags.split(',')]
                has_exemplar = 'exemplar' in tag_list
                new_tags = ['exemplar', 'paper', sf_slug] if has_exemplar else ['paper', sf_slug]
                new_content = re.sub(
                    r'^tags: \[.+?\]',
                    f'tags: [{", ".join(new_tags)}]',
                    content,
                    flags=re.MULTILINE,
                )
            else:
                new_content = content

            # ── Topics section: all valid psych topics ─────────────────────
            all_linked = topics_by_paper.get(oa_id, [])

            if all_linked:
                topic_lines = [f"- [[{t}]]" for t in all_linked]
            else:
                # Fallback: primary + subfield
                topic_lines = [f"- [[{primary}]]"]

            # Always include subfield as last link (connects to MOC)
            topic_lines.append(f"- [[{subfield}]]")

            block = "## Topics\n\n" + "\n".join(topic_lines) + "\n"
            new_content = re.sub(
                r'## Topics?\n\n[\s\S]*?(?=\n## |\Z)',
                block,
                new_content,
            )

            if new_content != content:
                fpath.write_text(new_content, encoding="utf-8")
                updated += 1
            else:
                skipped += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"\n  WARN {fpath.name}: {e}")

        if idx % 1000 == 0 or idx == total:
            print(f"  {idx:,}/{total:,}  updated={updated:,}  skipped={skipped:,}",
                  end="\r", flush=True)

    print(f"\n  Done.  updated={updated:,}  skipped={skipped:,}  errors={errors}")


# ---------------------------------------------------------------------------
# Step 3 — Update graph.json
# ---------------------------------------------------------------------------

def update_graph() -> None:
    cfg = json.loads(GRAPH_JSON.read_text(encoding="utf-8"))

    # Keep only non-paper colorGroups (topic colors, MOC colors, cross-disciplinary)
    non_paper_groups = [
        g for g in cfg["colorGroups"]
        if not g["query"].startswith("tag:paper")
    ]

    # Build 7 per-subfield paper colorGroups
    paper_groups = []
    for subfield, color in SUBFIELD_PAPER_COLORS.items():
        sf_slug = slugify(subfield)
        paper_groups.append({
            "query": f"tag:paper tag:{sf_slug}",
            "color": color,
        })

    cfg["colorGroups"] = non_paper_groups + paper_groups
    cfg["search"] = ""   # ensure papers are visible

    GRAPH_JSON.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    print(f"  graph.json updated  ({len(cfg['colorGroups'])} color groups)")
    print(f"  (replaced per-topic paper colors with {len(paper_groups)} per-subfield colors)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not DB_PATH.exists():
        sys.exit(f"ERROR: DB not found at {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA cache_size=-131072")

    # The 144 valid psychology topic names
    valid_topics: set[str] = {
        r["primary_topic"]
        for r in conn.execute(
            "SELECT DISTINCT primary_topic FROM papers WHERE primary_topic != ''"
        ).fetchall()
    }
    print(f"  {len(valid_topics)} valid psychology topics")

    print("\nStep 1+2 -- Restoring multi-topic links + fixing tags...")
    t0 = time.time()
    patch_papers(conn, valid_topics)
    print(f"  {time.time()-t0:.0f}s")

    print("\nStep 3 -- Updating graph.json...")
    update_graph()

    conn.close()
    print("\nDone. Reload Obsidian.")


if __name__ == "__main__":
    main()
