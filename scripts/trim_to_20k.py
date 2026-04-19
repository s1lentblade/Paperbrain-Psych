#!/usr/bin/env python3
"""
trim_to_20k.py — Trim Papers/ back to exactly 20,000 notes.

Strategy:
  1. Compute the same proportional 20K allocation per topic used in generate_papers.
  2. For each topic, read all paper notes' citation counts.
  3. Keep the top-cited alloc[topic] papers; delete the rest.
  4. For topics under quota (shouldn't happen), leave them alone.
"""

import re
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR   = Path(__file__).parent
DB_PATH      = SCRIPT_DIR.parent / "data" / "papers.db"
VAULT_PATH   = SCRIPT_DIR.parent / "full psychology breakdown"
PAPERS_DIR   = VAULT_PATH / "Papers"
TOTAL_TARGET = 20_000


def main() -> None:
    if not DB_PATH.exists():
        sys.exit(f"ERROR: DB not found at {DB_PATH}")

    # ── Compute proportional allocation ──────────────────────────────────────
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")

    topics_dist = conn.execute("""
        SELECT primary_topic, COUNT(*) AS n
        FROM papers
        WHERE primary_topic != '' AND title IS NOT NULL AND title != ''
        GROUP BY primary_topic ORDER BY n DESC
    """).fetchall()
    conn.close()

    grand_total = sum(t["n"] for t in topics_dist)
    alloc: dict[str, int] = {}
    for t in topics_dist:
        alloc[t["primary_topic"]] = max(1, round(t["n"] / grand_total * TOTAL_TARGET))

    diff = TOTAL_TARGET - sum(alloc.values())
    by_size = sorted(alloc, key=lambda k: alloc[k], reverse=True)
    for i in range(abs(diff)):
        k = by_size[i % len(by_size)]
        if diff > 0:
            alloc[k] += 1
        elif alloc[k] > 1:
            alloc[k] -= 1

    # ── Read all paper notes and group by topic ───────────────────────────────
    print("Reading paper notes...", flush=True)
    files = list(PAPERS_DIR.glob("*.md"))
    print(f"  {len(files):,} paper notes found")

    # Map: topic_name -> list of (citations, Path)
    topic_papers: dict[str, list[tuple[int, Path]]] = {}
    untagged: list[Path] = []

    for f in files:
        try:
            txt = f.read_text(encoding="utf-8", errors="ignore")
            topic_m = re.search(r'^topic: "(.+?)"', txt, re.MULTILINE)
            cit_m   = re.search(r'^citations: (\d+)', txt, re.MULTILINE)
            if not topic_m:
                untagged.append(f)
                continue
            topic = topic_m.group(1)
            cit   = int(cit_m.group(1)) if cit_m else 0
            topic_papers.setdefault(topic, []).append((cit, f))
        except Exception:
            untagged.append(f)

    print(f"  {len(topic_papers)} topics found  |  {len(untagged)} untagged files")

    # ── Delete lowest-cited excess per topic ─────────────────────────────────
    print("Trimming excess papers...", flush=True)
    deleted = kept = over_topics = 0

    for topic, papers in sorted(topic_papers.items()):
        quota = alloc.get(topic, 0)
        if len(papers) <= quota:
            kept += len(papers)
            continue

        over_topics += 1
        # Sort descending by citations, keep top quota
        papers.sort(key=lambda x: x[0], reverse=True)
        to_keep   = papers[:quota]
        to_delete = papers[quota:]

        kept    += len(to_keep)
        deleted += len(to_delete)
        for _, fpath in to_delete:
            fpath.unlink()

    print(f"  Deleted: {deleted:,}  (from {over_topics} over-quota topics)")
    print(f"  Kept:    {kept:,}")

    remaining = len(list(PAPERS_DIR.glob("*.md")))
    print(f"\n  Papers/ now contains: {remaining:,} notes")
    if untagged:
        print(f"  Note: {len(untagged)} untagged files left untouched")


if __name__ == "__main__":
    main()
