#!/usr/bin/env python3
"""
upgrade_to_20k.py

Step 1 — Patch every existing paper note:
          * Trim ## Topics section to primary topic + subfield only
          * Ensure tags include [paper, <topic-slug>]

Step 2 — Generate new paper notes until vault reaches ~20,000 total
          (top-cited per topic, proportionally allocated)

Step 3 — Update graph.json:
          * Clear search filter so papers appear
          * Tune force-layout to match the clean psychology vault
"""

import json
import re
import sqlite3
import sys
import time
from pathlib import Path

SCRIPT_DIR  = Path(__file__).parent
DB_PATH     = SCRIPT_DIR.parent / "data" / "papers.db"
VAULT_PATH  = SCRIPT_DIR.parent / "full psychology breakdown"
PAPERS_DIR  = VAULT_PATH / "Papers"
GRAPH_JSON  = VAULT_PATH / ".obsidian" / "graph.json"

TOTAL_TARGET = 20_000


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def sanitize(s: str, max_len: int = 100) -> str:
    s = re.sub(r'[\\/:*?"<>|#\^\[\]]', '', s)
    s = re.sub(r'\s+', ' ', s).strip().strip('.')
    return (s[:max_len] if len(s) > max_len else s) or 'Untitled'


def slugify(s: str) -> str:
    s = s.lower()
    s = re.sub(r'[^a-z0-9]+', '-', s)
    return s.strip('-')


def ys(v: object) -> str:
    return str(v).replace('"', "'")


def topic_section(topic: str, subfield: str) -> str:
    return f"## Topic\n\n- [[{topic}]]\n- [[{subfield}]]\n"


# ---------------------------------------------------------------------------
# Step 1 — Patch existing papers
# ---------------------------------------------------------------------------

def patch_existing(valid_topics: dict[str, str]) -> None:
    """
    valid_topics: topic_name -> subfield_name  (144 psych topics)
    Fixes every paper note already in Papers/:
      - Replaces '## Topics?' section with clean single-topic version
      - Ensures tags = [paper, <topic-slug>]  (preserves 'exemplar' if present)
    """
    files = list(PAPERS_DIR.glob("*.md"))
    print(f"  {len(files):,} existing notes to patch")

    updated = skipped = errors = 0
    total = len(files)

    for idx, fpath in enumerate(files, 1):
        try:
            content = fpath.read_text(encoding="utf-8", errors="ignore")

            # Pull topic + subfield from frontmatter
            topic_m    = re.search(r'^topic: "(.+?)"',    content, re.MULTILINE)
            subfield_m = re.search(r'^subfield: "(.+?)"', content, re.MULTILINE)
            tags_m     = re.search(r'^tags: \[(.+?)\]',   content, re.MULTILINE)

            if not topic_m:
                skipped += 1
                continue

            topic    = topic_m.group(1)
            subfield = subfield_m.group(1) if subfield_m else ""

            # Only patch psych-primary papers
            if topic not in valid_topics:
                skipped += 1
                continue

            slug = slugify(topic)
            new_content = content

            # --- Fix tags ---
            if tags_m:
                raw_tags = tags_m.group(1)
                tag_list = [t.strip().strip('"') for t in raw_tags.split(',')]
                # Keep 'exemplar' if present, ensure 'paper' and slug present
                keep = [t for t in tag_list if t not in ('paper',) and not (
                    t == slug or (t != 'exemplar' and '-' in t and t != slug)
                )]
                # Rebuild: always paper first, then slug, then exemplar if was there
                has_exemplar = 'exemplar' in tag_list
                new_tags = ['paper', slug]
                if has_exemplar:
                    new_tags = ['exemplar'] + new_tags
                new_tag_str = ', '.join(new_tags)
                new_content = re.sub(
                    r'^tags: \[.+?\]',
                    f'tags: [{new_tag_str}]',
                    new_content,
                    flags=re.MULTILINE,
                )

            # --- Fix topic section ---
            new_section = topic_section(topic, subfield)
            # Match "## Topic" or "## Topics" (with or without trailing content)
            new_content = re.sub(
                r'## Topics?\n\n[\s\S]*?(?=\n## |\Z)',
                new_section,
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
# Step 2 — Generate new papers
# ---------------------------------------------------------------------------

def generate_new(conn: sqlite3.Connection, valid_topics: dict[str, str]) -> None:
    # ── Collect existing openalex_ids so we don't duplicate ─────────────────
    print("  Scanning existing openalex_ids…", flush=True)
    existing_ids: set[str] = set()
    existing_fnames: set[str] = set()
    for f in PAPERS_DIR.glob("*.md"):
        existing_fnames.add(f.name)
        try:
            txt = f.read_text(encoding="utf-8", errors="ignore")
            m = re.search(r'^openalex_id: "(.+?)"', txt, re.MULTILINE)
            if m:
                existing_ids.add(m.group(1))
        except Exception:
            pass
    print(f"  {len(existing_ids):,} existing IDs  |  {len(existing_fnames):,} existing files")

    current_count = len(existing_fnames)
    if current_count >= TOTAL_TARGET:
        print(f"  Already at {current_count:,} notes — nothing to generate.")
        return

    need = TOTAL_TARGET - current_count
    print(f"  Need {need:,} more notes to reach {TOTAL_TARGET:,} total")

    # ── Proportional allocation ──────────────────────────────────────────────
    topics_dist = conn.execute("""
        SELECT primary_topic, COUNT(*) AS n
        FROM papers
        WHERE primary_topic != '' AND title IS NOT NULL AND title != ''
        GROUP BY primary_topic
        ORDER BY n DESC
    """).fetchall()

    grand_total = sum(t["n"] for t in topics_dist)
    alloc: dict[str, int] = {}
    for t in topics_dist:
        alloc[t["primary_topic"]] = max(1, round(t["n"] / grand_total * TOTAL_TARGET))

    # Trim/pad to hit target exactly
    diff = TOTAL_TARGET - sum(alloc.values())
    by_size = sorted(alloc, key=lambda k: alloc[k], reverse=True)
    for i in range(abs(diff)):
        k = by_size[i % len(by_size)]
        if diff > 0:
            alloc[k] += 1
        elif alloc[k] > 1:
            alloc[k] -= 1

    print(f"  Target allocation: {sum(alloc.values()):,}  "
          f"(min={min(alloc.values())} / max={max(alloc.values())} per topic)", flush=True)

    # ── Fetch candidates per topic ───────────────────────────────────────────
    # Fetch more than needed per topic to account for existing papers
    print("  Fetching candidate papers from DB…", flush=True)
    candidates: list[sqlite3.Row] = []
    for topic_name, target_n in alloc.items():
        # Fetch 2× target to ensure we have enough after skipping existing
        rows = conn.execute("""
            SELECT id, title, year, doi, cited_by_count, abstract,
                   primary_topic, subfield
            FROM papers
            WHERE primary_topic = ?
              AND title IS NOT NULL AND title != ''
            ORDER BY cited_by_count DESC
            LIMIT ?
        """, [topic_name, target_n * 2]).fetchall()
        candidates.extend(rows)

    print(f"  {len(candidates):,} candidates fetched", flush=True)

    # ── Bulk-fetch authors ───────────────────────────────────────────────────
    print("  Loading authors…", flush=True)
    paper_ids = [p["id"] for p in candidates]
    authors_by_paper: dict[str, list[str]] = {}
    chunk = 900
    for i in range(0, len(paper_ids), chunk):
        piece = paper_ids[i:i + chunk]
        ph = ",".join("?" * len(piece))
        rows = conn.execute(f"""
            SELECT paper_id, name, position
            FROM authors
            WHERE paper_id IN ({ph})
              AND position IN ('first', 'last')
            ORDER BY paper_id,
                     CASE position WHEN 'first' THEN 0 ELSE 1 END
        """, piece).fetchall()
        for r in rows:
            authors_by_paper.setdefault(r["paper_id"], []).append(r["name"])

    # ── Write notes (skip existing, stop when we hit per-topic quota) ────────
    print("  Writing new notes…", flush=True)
    written_per_topic: dict[str, int] = {}
    written = skipped_existing = skipped_quota = errors = 0

    for idx, p in enumerate(candidates, 1):
        topic    = p["primary_topic"] or ""
        subfield = p["subfield"] or ""
        oa_id    = p["id"] or ""

        # Skip if already in vault
        if oa_id in existing_ids:
            skipped_existing += 1
            continue

        # Stop if we've hit the per-topic quota
        topic_quota = alloc.get(topic, 0)
        if written_per_topic.get(topic, 0) >= topic_quota:
            skipped_quota += 1
            continue

        title    = (p["title"] or "Untitled").strip()
        year     = p["year"] or ""
        doi      = p["doi"] or ""
        cit      = p["cited_by_count"] or 0
        abstract = (p["abstract"] or "")[:700]
        authors  = authors_by_paper.get(oa_id, [])
        slug     = slugify(topic)

        fname = sanitize(f"{title} ({year})") + ".md"
        fpath = PAPERS_DIR / fname

        if fname in existing_fnames:
            skipped_existing += 1
            continue

        authors_yaml = "[" + ", ".join(f'"{ys(a)}"' for a in authors) + "]"
        authors_line = ", ".join(authors) if authors else ""
        doi_url = (f"https://doi.org/{doi}"
                   if doi and not doi.startswith("http") else doi)

        lines = [
            "---",
            f'title: "{ys(title)}"',
            f"authors: {authors_yaml}",
            f"tags: [paper, {slug}]",
            f"year: {year}",
            f'doi: "{doi}"',
            f"citations: {cit}",
            f'topic: "{ys(topic)}"',
            f'subfield: "{ys(subfield)}"',
            f'openalex_id: "{oa_id}"',
            "---",
            "",
            f"# {title}",
            "",
        ]
        if authors_line:
            lines.append(f"**Authors:** {authors_line}")
        lines += [f"**Year:** {year}", f"**Citations:** {cit:,}"]
        if doi_url:
            lines.append(f"**DOI:** {doi_url}")
        lines.append("")
        if abstract:
            lines += ["## Abstract", "", abstract, ""]
        lines.append(topic_section(topic, subfield))

        try:
            fpath.write_text("\n".join(lines), encoding="utf-8")
            written += 1
            written_per_topic[topic] = written_per_topic.get(topic, 0) + 1
            existing_fnames.add(fname)
            existing_ids.add(oa_id)
        except OSError as e:
            errors += 1
            if errors <= 5:
                print(f"\n  WARN: {fname!r}: {e}")

        if written % 1000 == 0 and written > 0:
            print(f"  written={written:,}", end="\r", flush=True)

    print(f"\n  Done.  written={written:,}  skipped_existing={skipped_existing:,}  errors={errors}")
    total_now = len(list(PAPERS_DIR.glob("*.md")))
    print(f"  Vault now contains: {total_now:,} paper notes")


# ---------------------------------------------------------------------------
# Step 3 — Update graph.json
# ---------------------------------------------------------------------------

def update_graph() -> None:
    cfg = json.loads(GRAPH_JSON.read_text(encoding="utf-8"))

    # Show papers (clear the filter)
    cfg["search"] = ""

    # Match the psychology vault's tuned force settings
    cfg["showOrphans"]         = False
    cfg["lineSizeMultiplier"]  = 0.1
    cfg["nodeSizeMultiplier"]  = 1.0
    cfg["centerStrength"]      = 0
    cfg["repelStrength"]       = 20
    cfg["linkStrength"]        = 1
    cfg["linkDistance"]        = 30

    GRAPH_JSON.write_text(json.dumps(cfg, indent=2), encoding="utf-8")
    print("  graph.json updated")


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

    # Build valid-topic map
    valid_topics: dict[str, str] = {
        r["primary_topic"]: r["subfield"]
        for r in conn.execute("""
            SELECT DISTINCT primary_topic, subfield FROM papers
            WHERE primary_topic != ''
        """).fetchall()
    }
    print(f"  {len(valid_topics)} valid psychology topics loaded")

    print("\nStep 1 -- Patching existing paper notes...")
    t0 = time.time()
    patch_existing(valid_topics)
    print(f"  {time.time()-t0:.0f}s")

    print("\nStep 2 -- Generating new papers to reach 20,000...")
    t0 = time.time()
    generate_new(conn, valid_topics)
    print(f"  {time.time()-t0:.0f}s")

    print("\nStep 3 -- Updating graph.json...")
    update_graph()

    conn.close()
    print("\nAll done. Reload Obsidian to see 20K papers in the graph.")


if __name__ == "__main__":
    main()
