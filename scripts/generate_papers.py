#!/usr/bin/env python3
"""
generate_papers.py — Populate vault/Papers/ with ~10K top-cited paper notes,
proportionally sampled by topic size.

Usage:
    python scripts/generate_papers.py               # generate ~10K notes
    python scripts/generate_papers.py --total 5000  # custom target
    python scripts/generate_papers.py --clear       # wipe existing first
"""

import argparse
import re
import sqlite3
import sys
from pathlib import Path

SCRIPT_DIR   = Path(__file__).parent
DB_PATH      = SCRIPT_DIR.parent / "data" / "papers.db"
VAULT_PATH   = SCRIPT_DIR.parent / "full psychology breakdown"
PAPERS_DIR   = VAULT_PATH / "Papers"
TOTAL_TARGET = 10_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize(s: str, max_len: int = 100) -> str:
    """Make a safe Windows filename."""
    s = re.sub(r'[\\/:*?"<>|#\^\[\]]', '', s)
    s = re.sub(r'\s+', ' ', s).strip().strip('.')
    return (s[:max_len] if len(s) > max_len else s) or 'Untitled'


def ys(v: object) -> str:
    """YAML-safe string — escape double quotes."""
    return str(v).replace('"', "'")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Generate PaperBrain paper notes")
    ap.add_argument('--total', type=int, default=TOTAL_TARGET,
                    help=f'Target note count (default {TOTAL_TARGET})')
    ap.add_argument('--clear', action='store_true',
                    help='Delete all existing .md files in Papers/ first')
    args = ap.parse_args()

    if not DB_PATH.exists():
        sys.exit(f'ERROR: DB not found at {DB_PATH}')
    if not VAULT_PATH.exists():
        sys.exit(f'ERROR: Vault not found at {VAULT_PATH}')

    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA query_only=ON")
    conn.execute("PRAGMA cache_size=-131072")   # 128 MB cache

    # ── Step 1: topic distribution ────────────────────────────────────────
    print('Fetching topic distribution…', flush=True)
    topics = conn.execute("""
        SELECT primary_topic, COUNT(*) AS n
        FROM papers
        WHERE primary_topic != '' AND title IS NOT NULL AND title != ''
        GROUP BY primary_topic
        ORDER BY n DESC
    """).fetchall()

    if not topics:
        sys.exit('ERROR: No topics found in DB.')

    grand_total = sum(t['n'] for t in topics)
    print(f'  {len(topics)} topics  |  {grand_total:,} papers in DB', flush=True)

    # ── Step 2: proportional allocation (min 1 per topic) ─────────────────
    alloc: dict[str, int] = {}
    for t in topics:
        alloc[t['primary_topic']] = max(1, round(t['n'] / grand_total * args.total))

    # Trim/pad to hit target exactly — adjust largest topics first
    diff = args.total - sum(alloc.values())
    by_size = sorted(alloc, key=lambda k: alloc[k], reverse=True)
    for i in range(abs(diff)):
        k = by_size[i % len(by_size)]
        if diff > 0:
            alloc[k] += 1
        elif alloc[k] > 1:
            alloc[k] -= 1

    allocated = sum(alloc.values())
    print(f'  Allocated {allocated:,} slots  '
          f'(min {min(alloc.values())} / max {max(alloc.values())} per topic)', flush=True)

    # ── Step 3: optional clear ────────────────────────────────────────────
    PAPERS_DIR.mkdir(parents=True, exist_ok=True)
    if args.clear:
        removed = 0
        for f in PAPERS_DIR.glob('*.md'):
            f.unlink()
            removed += 1
        print(f'  Cleared {removed} existing notes', flush=True)

    # ── Step 4: fetch papers per topic, collect IDs ───────────────────────
    print('Fetching top-cited papers per topic…', flush=True)
    all_papers: list[sqlite3.Row] = []
    for topic_name, n in alloc.items():
        rows = conn.execute("""
            SELECT id, title, year, doi, cited_by_count, abstract,
                   primary_topic, subfield
            FROM papers
            WHERE primary_topic = ?
              AND title IS NOT NULL AND title != ''
            ORDER BY cited_by_count DESC
            LIMIT ?
        """, [topic_name, n]).fetchall()
        all_papers.extend(rows)

    print(f'  Fetched {len(all_papers):,} paper rows', flush=True)

    # ── Step 5: bulk-fetch authors (chunked to stay under SQLite limit) ───
    print('Loading authors…', flush=True)
    paper_ids = [p['id'] for p in all_papers]
    authors_by_paper: dict[str, list[str]] = {}
    chunk_size = 900
    for start in range(0, len(paper_ids), chunk_size):
        chunk = paper_ids[start:start + chunk_size]
        ph = ','.join('?' * len(chunk))
        rows = conn.execute(f"""
            SELECT paper_id, name, position
            FROM authors
            WHERE paper_id IN ({ph})
              AND position IN ('first', 'last')
            ORDER BY paper_id,
                     CASE position WHEN 'first' THEN 0 ELSE 1 END
        """, chunk).fetchall()
        for r in rows:
            authors_by_paper.setdefault(r['paper_id'], []).append(r['name'])

    # ── Step 6: write markdown files ──────────────────────────────────────
    print('Writing notes…', flush=True)
    written = skipped = errors = 0

    for idx, p in enumerate(all_papers, 1):
        title    = (p['title'] or 'Untitled').strip()
        year     = p['year'] or ''
        doi      = p['doi'] or ''
        cit      = p['cited_by_count'] or 0
        abstract = (p['abstract'] or '')[:700]
        topic    = p['primary_topic'] or ''
        subfield = p['subfield'] or ''
        oa_id    = p['id'] or ''
        authors  = authors_by_paper.get(oa_id, [])

        fname = sanitize(f"{title} ({year})") + '.md'
        fpath = PAPERS_DIR / fname

        if fpath.exists():
            skipped += 1
            continue

        authors_yaml = '[' + ', '.join(f'"{ys(a)}"' for a in authors) + ']'
        authors_line = ', '.join(authors) if authors else ''

        doi_url = (f'https://doi.org/{doi}'
                   if doi and not doi.startswith('http') else doi)

        lines = [
            '---',
            f'title: "{ys(title)}"',
            f'authors: {authors_yaml}',
            f'tags: [paper]',
            f'year: {year}',
            f'doi: "{doi}"',
            f'citations: {cit}',
            f'topic: "{ys(topic)}"',
            f'subfield: "{ys(subfield)}"',
            f'openalex_id: "{oa_id}"',
            '---',
            '',
            f'# {title}',
            '',
        ]

        if authors_line:
            lines.append(f'**Authors:** {authors_line}')
        lines += [
            f'**Year:** {year}',
            f'**Citations:** {cit:,}',
        ]
        if doi_url:
            lines.append(f'**DOI:** {doi_url}')
        lines.append('')

        if abstract:
            lines += ['## Abstract', '', abstract, '']

        lines += [
            '## Topic',
            '',
            f'- [[{topic}]]',
            f'- [[{subfield}]]',
            '',
        ]

        try:
            fpath.write_text('\n'.join(lines), encoding='utf-8')
            written += 1
        except OSError as e:
            errors += 1
            if errors <= 5:
                print(f'\n  WARN: could not write {fname!r}: {e}', flush=True)

        if idx % 1000 == 0 or idx == len(all_papers):
            pct = idx / len(all_papers) * 100
            print(f'  {idx:,}/{len(all_papers):,} ({pct:.0f}%)  '
                  f'written={written:,}  skipped={skipped:,}',
                  end='\r', flush=True)

    print(f'\n\nDone.')
    print(f'  Written : {written:,}')
    print(f'  Skipped : {skipped:,}  (already existed)')
    print(f'  Errors  : {errors}')
    print(f'  Total in Papers/ : ~{written + skipped:,}')
    conn.close()


if __name__ == '__main__':
    main()
