#!/usr/bin/env python3
"""
restore_crossdisciplinary.py

Step 1 — Create minimal stub files in Topics/ for all 4,079 cross-disciplinary
          topics referenced in paper_topics but missing from the vault.
Step 2 — Add tag:cross-disciplinary color group to graph.json (dim neutral).
Step 3 — Restore full multi-topic wikilinks in every paper note, using the
          exact sanitized filename stem so all links resolve correctly.
"""

import json
import re
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR.parent / "data" / "papers.db"
VAULT_PATH = SCRIPT_DIR.parent / "full psychology breakdown"
TOPICS_DIR = VAULT_PATH / "Topics"
PAPERS_DIR = VAULT_PATH / "Papers"
GRAPH_JSON = VAULT_PATH / ".obsidian" / "graph.json"

# Dim neutral blue-gray — visible but clearly secondary to psychology clusters
CROSS_COLOR = {"a": 0.55, "rgb": 3552822}   # ~#363636 dark gray-blue


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize(s: str, max_len: int = 100) -> str:
    """Windows-safe filename stem, preserving enough of the original name."""
    s = re.sub(r'[\\/:*?"<>|#\^\[\]]', '', s)
    s = re.sub(r'\s+', ' ', s).strip().strip('.')
    return (s[:max_len] if len(s) > max_len else s) or 'Untitled'


def ys(v: object) -> str:
    return str(v).replace('"', "'")


# ---------------------------------------------------------------------------
# Step 1 — Create stub files, build raw_name → file_stem mapping
# ---------------------------------------------------------------------------

def create_stubs(conn: sqlite3.Connection) -> dict[str, str]:
    """
    Returns topic_name → file_stem mapping for ALL topics
    (both existing 144 and new stubs).
    """
    # All unique topic names ever referenced
    all_topics = {r["topic_name"] for r in conn.execute(
        "SELECT DISTINCT topic_name FROM paper_topics"
    ).fetchall()}
    print(f"  Total unique topics in paper_topics: {len(all_topics):,}")

    # Existing topic note stems
    existing_stems: dict[str, Path] = {f.stem: f for f in TOPICS_DIR.glob("*.md")}

    # Build mapping: existing notes → identity (stem == topic name for the 144)
    mapping: dict[str, str] = {}
    for stem in existing_stems:
        mapping[stem] = stem           # stem IS the topic name for existing notes

    # Create stubs for missing topics
    created = skipped_exists = collisions = 0

    for topic_name in sorted(all_topics):
        if topic_name in mapping:      # already handled (exact name match)
            continue

        stem = sanitize(topic_name)

        # Collision handling: two raw names may sanitize to the same stem
        candidate = stem
        counter = 0
        while candidate in existing_stems and existing_stems[candidate].stem != topic_name:
            counter += 1
            candidate = f"{stem} ({counter})"
        stem = candidate

        mapping[topic_name] = stem
        fpath = TOPICS_DIR / (stem + ".md")

        if fpath.exists():
            skipped_exists += 1
            existing_stems[stem] = fpath
            continue

        content = "\n".join([
            "---",
            f'title: "{ys(topic_name)}"',
            "tags: [cross-disciplinary]",
            "---",
            "",
        ])
        fpath.write_text(content, encoding="utf-8")
        existing_stems[stem] = fpath
        created += 1

    print(f"  Stub files created: {created:,}  |  already existed: {skipped_exists:,}")
    return mapping


# ---------------------------------------------------------------------------
# Step 2 — Update graph.json
# ---------------------------------------------------------------------------

def update_graph(n_paper_groups_before: int) -> None:
    cfg = json.loads(GRAPH_JSON.read_text(encoding="utf-8"))

    # Remove any previous cross-disciplinary entry
    cfg["colorGroups"] = [
        g for g in cfg["colorGroups"]
        if "cross-disciplinary" not in g["query"]
    ]

    # Insert at position 0 so it's evaluated first (lowest priority for
    # Obsidian means last match wins, but first entry is fine as fallback)
    cfg["colorGroups"].insert(0, {
        "query": "tag:cross-disciplinary",
        "color": CROSS_COLOR,
    })

    GRAPH_JSON.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    print(f"  graph.json updated  ({len(cfg['colorGroups'])} color groups)")


# ---------------------------------------------------------------------------
# Step 3 — Restore full wikilinks in paper notes
# ---------------------------------------------------------------------------

def restore_wikilinks(conn: sqlite3.Connection, mapping: dict[str, str]) -> None:
    files = list(PAPERS_DIR.glob("*.md"))
    print(f"  {len(files):,} paper notes to process")

    # Collect openalex IDs
    id_to_file: dict[str, Path] = {}
    for f in files:
        try:
            txt = f.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r'^openalex_id: "(.+?)"', txt, re.MULTILINE)
            if m:
                id_to_file[m.group(1)] = f
        except Exception:
            pass

    # Bulk-fetch paper_topics — all topics (mapping ensures files exist)
    all_ids = list(id_to_file.keys())
    topics_by_paper: dict[str, list[tuple[str, int]]] = {}
    chunk = 900
    for i in range(0, len(all_ids), chunk):
        piece = all_ids[i : i + chunk]
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
            if r["topic_name"] in mapping:   # has a file (all should now)
                topics_by_paper.setdefault(r["paper_id"], []).append(
                    (r["topic_name"], r["is_primary"])
                )

    updated = skipped = errors = 0
    total = len(files)

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

            lines = []
            for raw_name, is_prim in all_linked:
                stem   = mapping.get(raw_name, sanitize(raw_name))
                marker = " *(primary)*" if is_prim else ""
                lines.append(f"- [[{stem}]]{marker}")

            if not lines:
                lines.append(f"- [[{mapping.get(primary, primary)}]] *(primary)*")
            if subfield:
                lines.append(f"- [[{subfield}]]")

            block = "## Topics\n\n" + "\n".join(lines) + "\n"
            new   = re.sub(r"## Topics?\n\n[\s\S]*?(?=\n## |\Z)", block, content)

            if new != content:
                fpath.write_text(new, encoding="utf-8")
                updated += 1
            else:
                skipped += 1

        except Exception as e:
            errors += 1
            if errors <= 5:
                print(f"\n  WARN {fpath.name}: {e}")

        if idx % 1000 == 0 or idx == total:
            print(
                f"  {idx:,}/{total:,}  updated={updated:,}  skipped={skipped:,}",
                end="\r", flush=True,
            )

    print(f"\n  Done.  updated={updated:,}  skipped={skipped:,}  errors={errors}")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    if not DB_PATH.exists():
        sys.exit(f"ERROR: DB not found at {DB_PATH}")

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")

    print("Step 1 — Creating cross-disciplinary stub files…")
    mapping = create_stubs(conn)
    print(f"  Total topic mapping entries: {len(mapping):,}")

    print("\nStep 2 — Updating graph.json…")
    update_graph(0)

    print("\nStep 3 — Restoring full topic wikilinks in paper notes…")
    restore_wikilinks(conn, mapping)

    conn.close()
    print("\nAll done. Reload Obsidian and toggle Brain View.")


if __name__ == "__main__":
    main()
