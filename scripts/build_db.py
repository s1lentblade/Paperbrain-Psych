"""
build_db.py — Load 6.6M Psychology papers from JSONL into SQLite

Usage:
    python scripts/build_db.py [--workers N] [--batch-size N]

Input:  data/by_year/papers_*.jsonl  (225 files, ~28 GB)
Output: data/papers.db

Strategy:
    1. Greedy bin-packing assigns files to N worker processes by file size.
    2. Each worker builds a private temp SQLite (data/db_parts/temp_N.db).
    3. Main process merges all temp DBs into final papers.db via ATTACH/INSERT.
    4. FTS5 virtual table built once at the end.

Schema:
    papers(id PK, title, year, doi, cited_by_count, type, abstract,
           primary_topic, subfield)
    authors(paper_id, name, position)          — no PK, bulk-inserted
    keywords(paper_id, keyword)                — no PK, bulk-inserted
    paper_topics(paper_id, topic_id, topic_name, is_primary)
    papers_fts  FTS5 on title + abstract

Performance knobs (WAL + synchronous=OFF during bulk load):
    Each temp DB: journal_mode=WAL, synchronous=OFF, cache_size=-524288 (~512 MB)
"""

import argparse
import json
import logging
import os
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

SCRIPT_DIR  = Path(__file__).parent
DATA_DIR    = SCRIPT_DIR.parent / "data"
BY_YEAR_DIR = DATA_DIR / "by_year"
PARTS_DIR   = DATA_DIR / "db_parts"
FINAL_DB    = DATA_DIR / "papers.db"

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

DEFAULT_WORKERS   = 8
DEFAULT_BATCH     = 10_000   # rows per executemany call


# ---------------------------------------------------------------------------
# Greedy bin-packing
# ---------------------------------------------------------------------------

def _assign_files(files_with_sizes: list[tuple[Path, int]], n_workers: int) -> list[list[str]]:
    """Size-aware assignment: largest files first, always assign to least-loaded worker."""
    buckets      = [[] for _ in range(n_workers)]
    bucket_bytes = [0]  * n_workers
    for path, size in sorted(files_with_sizes, key=lambda x: -x[1]):
        idx = bucket_bytes.index(min(bucket_bytes))
        buckets[idx].append(str(path))
        bucket_bytes[idx] += size
    total_gb = sum(s for _, s in files_with_sizes) / 1024**3
    print(f"  Load balance across {n_workers} workers (total {total_gb:.1f} GB):", flush=True)
    for i, (b, sz) in enumerate(zip(buckets, bucket_bytes)):
        print(f"    worker {i}: {len(b)} files, {sz/1024**3:.2f} GB", flush=True)
    return buckets


# ---------------------------------------------------------------------------
# Schema helpers
# ---------------------------------------------------------------------------

CREATE_PAPERS = """
CREATE TABLE IF NOT EXISTS papers (
    id              TEXT PRIMARY KEY,
    title           TEXT,
    year            INTEGER,
    doi             TEXT,
    cited_by_count  INTEGER,
    type            TEXT,
    abstract        TEXT,
    primary_topic   TEXT,
    subfield        TEXT
)
"""

CREATE_AUTHORS = """
CREATE TABLE IF NOT EXISTS authors (
    paper_id    TEXT NOT NULL,
    name        TEXT NOT NULL,
    position    TEXT
)
"""

CREATE_KEYWORDS = """
CREATE TABLE IF NOT EXISTS keywords (
    paper_id    TEXT NOT NULL,
    keyword     TEXT NOT NULL
)
"""

CREATE_PAPER_TOPICS = """
CREATE TABLE IF NOT EXISTS paper_topics (
    paper_id    TEXT NOT NULL,
    topic_id    TEXT,
    topic_name  TEXT,
    is_primary  INTEGER   -- 1 = primary topic, 0 = secondary
)
"""

CREATE_FTS = """
CREATE VIRTUAL TABLE IF NOT EXISTS papers_fts USING fts5(
    id UNINDEXED,
    title,
    abstract,
    content='papers',
    content_rowid='rowid'
)
"""

FINAL_INDEXES = [
    "CREATE INDEX IF NOT EXISTS idx_papers_year        ON papers(year)",
    "CREATE INDEX IF NOT EXISTS idx_papers_topic       ON papers(primary_topic)",
    "CREATE INDEX IF NOT EXISTS idx_papers_subfield    ON papers(subfield)",
    "CREATE INDEX IF NOT EXISTS idx_papers_citations   ON papers(cited_by_count)",
    "CREATE INDEX IF NOT EXISTS idx_authors_paper      ON authors(paper_id)",
    "CREATE INDEX IF NOT EXISTS idx_keywords_paper     ON keywords(paper_id)",
    "CREATE INDEX IF NOT EXISTS idx_keywords_kw        ON keywords(keyword)",
    "CREATE INDEX IF NOT EXISTS idx_topics_paper       ON paper_topics(paper_id)",
    "CREATE INDEX IF NOT EXISTS idx_topics_name        ON paper_topics(topic_name)",
]


def _apply_fast_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA synchronous=OFF")
    conn.execute("PRAGMA cache_size=-524288")   # 512 MB page cache
    conn.execute("PRAGMA temp_store=MEMORY")
    conn.execute("PRAGMA locking_mode=EXCLUSIVE")


def _create_schema(conn: sqlite3.Connection) -> None:
    conn.execute(CREATE_PAPERS)
    conn.execute(CREATE_AUTHORS)
    conn.execute(CREATE_KEYWORDS)
    conn.execute(CREATE_PAPER_TOPICS)
    conn.commit()


# ---------------------------------------------------------------------------
# Abstract reconstruction (only needed if inverted-index wasn't already
# converted by fetch_papers.py, kept for safety)
# ---------------------------------------------------------------------------

def _reconstruct_abstract(inv: dict | None) -> str:
    if not inv:
        return ""
    max_pos = max((p for positions in inv.values() for p in positions), default=-1)
    if max_pos < 0:
        return ""
    tokens = [""] * (max_pos + 1)
    for word, positions in inv.items():
        for pos in positions:
            tokens[pos] = word
    return " ".join(t for t in tokens if t)


# ---------------------------------------------------------------------------
# Worker — builds one temp DB
# ---------------------------------------------------------------------------

def _worker(worker_id: int, file_paths: list[str], parts_dir: str, batch_size: int) -> dict:
    """
    Process a list of JSONL files and write results to a temp SQLite DB.
    Returns a summary dict: {worker_id, papers, authors, keywords, topics, elapsed}.
    """
    t0      = time.time()
    db_path = Path(parts_dir) / f"temp_{worker_id}.db"

    conn = sqlite3.connect(str(db_path))
    _apply_fast_pragmas(conn)
    _create_schema(conn)

    papers_buf   = []
    authors_buf  = []
    keywords_buf = []
    topics_buf   = []

    total_papers   = 0
    total_authors  = 0
    total_keywords = 0
    total_topics   = 0

    def _flush():
        nonlocal total_papers, total_authors, total_keywords, total_topics
        if papers_buf:
            conn.executemany(
                "INSERT OR IGNORE INTO papers VALUES (?,?,?,?,?,?,?,?,?)",
                papers_buf
            )
            total_papers += len(papers_buf)
            papers_buf.clear()
        if authors_buf:
            conn.executemany(
                "INSERT INTO authors VALUES (?,?,?)",
                authors_buf
            )
            total_authors += len(authors_buf)
            authors_buf.clear()
        if keywords_buf:
            conn.executemany(
                "INSERT INTO keywords VALUES (?,?)",
                keywords_buf
            )
            total_keywords += len(keywords_buf)
            keywords_buf.clear()
        if topics_buf:
            conn.executemany(
                "INSERT INTO paper_topics VALUES (?,?,?,?)",
                topics_buf
            )
            total_topics += len(topics_buf)
            topics_buf.clear()
        conn.commit()

    for file_path in file_paths:
        with open(file_path, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    p = json.loads(line)
                except json.JSONDecodeError:
                    continue

                pid   = p.get("id") or ""
                title = (p.get("title") or "").strip()
                year  = p.get("publication_year")
                doi   = p.get("doi") or ""
                cites = p.get("cited_by_count") or 0
                ptype = p.get("type") or ""

                # Abstract: may be plain string (from fetch_papers reconstruction)
                # or inverted-index dict (older format) — handle both
                raw_abstract = p.get("abstract")
                if isinstance(raw_abstract, dict):
                    abstract = _reconstruct_abstract(raw_abstract)
                elif isinstance(raw_abstract, str):
                    abstract = raw_abstract
                else:
                    abstract = _reconstruct_abstract(p.get("abstract_inverted_index"))

                # Primary topic
                pt_obj   = p.get("primary_topic") or {}
                ptopic   = (pt_obj.get("display_name") or "").strip()
                sf_obj   = pt_obj.get("subfield") or {}
                subfield = (sf_obj.get("display_name") or "").strip()

                papers_buf.append((pid, title, year, doi, cites, ptype,
                                   abstract, ptopic, subfield))

                # Authors
                for auth in (p.get("authorships") or []):
                    name = ((auth.get("author") or {}).get("display_name") or "").strip()
                    pos  = auth.get("author_position") or ""
                    if name:
                        authors_buf.append((pid, name, pos))

                # Keywords
                for kw in (p.get("keywords") or []):
                    kname = (kw.get("display_name") or "").strip()
                    if kname:
                        keywords_buf.append((pid, kname))

                # All topics
                primary_id = pt_obj.get("id") or ""
                for t in (p.get("topics") or []):
                    tid    = t.get("id") or ""
                    tname  = (t.get("display_name") or "").strip()
                    is_pri = 1 if tid == primary_id else 0
                    if tname:
                        topics_buf.append((pid, tid, tname, is_pri))

                if len(papers_buf) >= batch_size:
                    _flush()

    _flush()
    conn.close()

    elapsed = time.time() - t0
    return {
        "worker_id": worker_id,
        "papers":    total_papers,
        "authors":   total_authors,
        "keywords":  total_keywords,
        "topics":    total_topics,
        "elapsed":   elapsed,
        "db_path":   str(db_path),
    }


# ---------------------------------------------------------------------------
# Merge temp DBs into final DB
# ---------------------------------------------------------------------------

def _merge_parts(part_dbs: list[str], final_path: Path) -> None:
    print(f"\nMerging {len(part_dbs)} temp DBs into {final_path} ...", flush=True)
    conn = sqlite3.connect(str(final_path))
    _apply_fast_pragmas(conn)
    _create_schema(conn)

    for i, part_path in enumerate(part_dbs, 1):
        print(f"  Attaching part {i}/{len(part_dbs)}: {Path(part_path).name}", flush=True)
        conn.execute(f"ATTACH DATABASE '{part_path}' AS part")
        conn.execute("INSERT OR IGNORE INTO papers SELECT * FROM part.papers")
        conn.execute("INSERT INTO authors      SELECT * FROM part.authors")
        conn.execute("INSERT INTO keywords     SELECT * FROM part.keywords")
        conn.execute("INSERT INTO paper_topics SELECT * FROM part.paper_topics")
        conn.commit()
        conn.execute("DETACH DATABASE part")

    print("\nBuilding indexes ...", flush=True)
    for ddl in FINAL_INDEXES:
        conn.execute(ddl)
    conn.commit()

    print("Building FTS5 index ...", flush=True)
    conn.execute(CREATE_FTS)
    conn.execute("INSERT INTO papers_fts(papers_fts) VALUES('rebuild')")
    conn.commit()

    # Row counts
    papers_n   = conn.execute("SELECT COUNT(*) FROM papers").fetchone()[0]
    authors_n  = conn.execute("SELECT COUNT(*) FROM authors").fetchone()[0]
    keywords_n = conn.execute("SELECT COUNT(*) FROM keywords").fetchone()[0]
    topics_n   = conn.execute("SELECT COUNT(*) FROM paper_topics").fetchone()[0]

    conn.close()
    db_size_gb = final_path.stat().st_size / 1024**3

    print(f"""
=======================================================
Database ready: {final_path}
  {papers_n:>12,}  papers
  {authors_n:>12,}  author rows
  {keywords_n:>12,}  keyword rows
  {topics_n:>12,}  topic rows
  {db_size_gb:.2f} GB on disk
=======================================================
""", flush=True)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    ap = argparse.ArgumentParser(description="Build SQLite DB from OpenAlex JSONL")
    ap.add_argument("--workers",    type=int, default=DEFAULT_WORKERS,
                    help=f"parallel worker processes (default: {DEFAULT_WORKERS})")
    ap.add_argument("--batch-size", type=int, default=DEFAULT_BATCH,
                    help=f"rows per executemany call (default: {DEFAULT_BATCH})")
    ap.add_argument("--keep-parts", action="store_true",
                    help="keep temp part DBs after merge (for debugging)")
    args = ap.parse_args()

    PARTS_DIR.mkdir(parents=True, exist_ok=True)

    # Collect files with sizes
    jsonl_files = sorted(BY_YEAR_DIR.glob("papers_*.jsonl"))
    if not jsonl_files:
        print(f"ERROR: No JSONL files found in {BY_YEAR_DIR}", file=sys.stderr)
        sys.exit(1)

    files_with_sizes = [(p, p.stat().st_size) for p in jsonl_files]
    total_gb = sum(s for _, s in files_with_sizes) / 1024**3
    print(f"Found {len(jsonl_files)} year files ({total_gb:.1f} GB total)", flush=True)

    n_workers = min(args.workers, len(jsonl_files))
    buckets   = _assign_files(files_with_sizes, n_workers)

    print(f"\nLaunching {n_workers} worker processes ...", flush=True)
    t_start = time.time()

    part_dbs  = []
    summaries = []

    with ProcessPoolExecutor(max_workers=n_workers) as pool:
        futures = {
            pool.submit(_worker, i, bucket, str(PARTS_DIR), args.batch_size): i
            for i, bucket in enumerate(buckets)
            if bucket   # skip empty buckets (n_workers > n_files)
        }
        for fut in as_completed(futures):
            try:
                result = fut.result()
            except Exception as exc:
                wid = futures[fut]
                print(f"  Worker {wid} FAILED: {exc}", flush=True)
                raise
            summaries.append(result)
            wid = result["worker_id"]
            print(
                f"  Worker {wid} done — "
                f"{result['papers']:,} papers in {result['elapsed']:.0f}s",
                flush=True
            )
            part_dbs.append(result["db_path"])

    # Sort by worker_id for deterministic merge order
    part_dbs.sort(key=lambda p: int(Path(p).stem.split("_")[1]))

    total_papers = sum(r["papers"] for r in summaries)
    elapsed_pass = time.time() - t_start
    print(f"\nAll workers done — {total_papers:,} papers in {elapsed_pass:.0f}s "
          f"({total_papers/elapsed_pass:,.0f} papers/s)", flush=True)

    _merge_parts(part_dbs, FINAL_DB)

    if not args.keep_parts:
        print("Removing temp part DBs ...", flush=True)
        for p in part_dbs:
            try:
                Path(p).unlink()
            except OSError:
                pass

    total_elapsed = time.time() - t_start
    print(f"Total time: {total_elapsed/60:.1f} min", flush=True)


if __name__ == "__main__":
    # Ensure child processes don't re-run main on Windows
    import multiprocessing
    multiprocessing.freeze_support()
    main()
