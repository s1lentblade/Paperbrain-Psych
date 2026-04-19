#!/usr/bin/env python3
"""
relink_all_topics.py

Re-links every paper note to ALL its topics — both the 144 psychology topics
and the 4,075 cross-disciplinary stubs — so the full knowledge graph is visible.

Step 1 — Build title -> file-stem mapping from Topics/ (handles the 42 cases
          where sanitization changed the filename).

Step 2 — Bulk-fetch paper_topics for all vault papers from the DB.

Step 3 — Rewrite each paper's ## Topics section with correct wikilink stems.
"""

import re
import sqlite3
import sys
import time
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR.parent / "data" / "papers.db"
VAULT_PATH = SCRIPT_DIR.parent / "full psychology breakdown"
PAPERS_DIR = VAULT_PATH / "Papers"
TOPICS_DIR = VAULT_PATH / "Topics"


# ---------------------------------------------------------------------------
# Step 1 — Build topic-name -> file-stem mapping
# ---------------------------------------------------------------------------

def build_mapping() -> dict[str, str]:
    mapping: dict[str, str] = {}
    for f in TOPICS_DIR.glob("*.md"):
        try:
            txt = f.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r'^title: "(.+?)"', txt, re.MULTILINE)
            title = m.group(1) if m else f.stem
            mapping[title] = f.stem
        except Exception:
            mapping[f.stem] = f.stem
    return mapping


# ---------------------------------------------------------------------------
# Step 2+3 — Fetch all paper_topics and rewrite notes
# ---------------------------------------------------------------------------

def relink(conn: sqlite3.Connection, mapping: dict[str, str]) -> None:
    files = list(PAPERS_DIR.glob("*.md"))
    total = len(files)
    print(f"  {total:,} paper notes to process")

    # Collect openalex_ids
    id_to_file: dict[str, Path] = {}
    for f in files:
        try:
            txt = f.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r'^openalex_id: "(.+?)"', txt, re.MULTILINE)
            if m:
                id_to_file[m.group(1)] = f
        except Exception:
            pass

    print(f"  {len(id_to_file):,} papers have openalex_id", flush=True)

    # Bulk-fetch ALL topics for these papers (no filter — include cross-disciplinary)
    print("  Loading paper_topics from DB...", flush=True)
    all_ids = list(id_to_file.keys())
    topics_by_paper: dict[str, list[str]] = {}
    chunk = 900
    for i in range(0, len(all_ids), chunk):
        piece = all_ids[i:i + chunk]
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
            # Only include topics that have a file in Topics/
            if r["topic_name"] in mapping:
                topics_by_paper.setdefault(r["paper_id"], []).append(r["topic_name"])

    print(f"  {len(topics_by_paper):,} papers have topic data", flush=True)

    # Rewrite notes
    updated = skipped = errors = 0
    for idx, fpath in enumerate(files, 1):
        try:
            content = fpath.read_text(encoding="utf-8", errors="ignore")

            topic_m    = re.search(r'^topic: "(.+?)"',       content, re.MULTILINE)
            subfield_m = re.search(r'^subfield: "(.+?)"',    content, re.MULTILINE)
            oa_m       = re.search(r'^openalex_id: "(.+?)"', content, re.MULTILINE)

            if not topic_m:
                skipped += 1
                continue

            primary  = topic_m.group(1)
            subfield = subfield_m.group(1) if subfield_m else ""
            oa_id    = oa_m.group(1) if oa_m else ""

            all_linked = topics_by_paper.get(oa_id, [])

            if all_linked:
                lines = [f"- [[{mapping.get(t, t)}]]" for t in all_linked]
            else:
                lines = [f"- [[{mapping.get(primary, primary)}]]"]

            # Always close with subfield MOC link
            if subfield:
                lines.append(f"- [[{subfield}]]")

            block = "## Topics\n\n" + "\n".join(lines) + "\n"
            new_content = re.sub(
                r"## Topics?\n\n[\s\S]*?(?=\n## |\Z)",
                block,
                content,
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
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not DB_PATH.exists():
        sys.exit(f"ERROR: DB not found at {DB_PATH}")

    print("Step 1 -- Building topic name -> file stem mapping...")
    mapping = build_mapping()
    print(f"  {len(mapping):,} topic files mapped")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA cache_size=-131072")

    print("\nStep 2+3 -- Relinking all paper topics...")
    t0 = time.time()
    relink(conn, mapping)
    print(f"  {time.time()-t0:.0f}s")

    conn.close()
    print("\nDone. Reload Obsidian.")


if __name__ == "__main__":
    main()
