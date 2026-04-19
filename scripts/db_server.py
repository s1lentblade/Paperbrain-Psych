"""
db_server.py — Local HTTP API server for PaperBrain SQLite database

Usage:
    python scripts/db_server.py              # run in foreground
    python scripts/db_server.py --start      # launch as detached background process
    python scripts/db_server.py --stop       # kill background process
    python scripts/db_server.py --port 27182 # custom port (default: 27182)

Endpoints:
    GET /health
    GET /search?q=TEXT&limit=20&year_min=&year_max=
    GET /topic?name=TEXT&limit=25&sort=citations|year_desc|year_asc&year_min=&year_max=&offset=0
    GET /paper?id=W1234567890
    GET /bridges?name=TEXT&limit=10
"""

import argparse
import json
import os
import sqlite3
import sys
import threading
import urllib.parse
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR.parent / "data" / "papers.db"
PID_FILE   = SCRIPT_DIR.parent / "data" / "db_server.pid"
LOG_FILE   = SCRIPT_DIR.parent / "data" / "db_server.log"
DEFAULT_PORT = 27182

# ---------------------------------------------------------------------------
# Database helpers (single connection, lock-protected — single-user server)
# ---------------------------------------------------------------------------

_conn: sqlite3.Connection | None = None
_db_lock = threading.Lock()


def _get_conn() -> sqlite3.Connection:
    global _conn
    if _conn is None:
        _conn = sqlite3.connect(str(DB_PATH), check_same_thread=False)
        _conn.row_factory = sqlite3.Row
        _conn.execute("PRAGMA journal_mode=WAL")
        _conn.execute("PRAGMA cache_size=-524288")   # 512 MB page cache
        _conn.execute("PRAGMA temp_store=MEMORY")
    return _conn


def _query_one(sql: str, params: list = []) -> dict | None:
    with _db_lock:
        cur = _get_conn().execute(sql, params)
        row = cur.fetchone()
        return dict(row) if row else None


def _query_all(sql: str, params: list = []) -> list[dict]:
    with _db_lock:
        cur = _get_conn().execute(sql, params)
        return [dict(r) for r in cur.fetchall()]


def _fts_escape(q: str) -> str:
    """Wrap each word in FTS5 double-quote phrases to prevent syntax errors."""
    words = [w.strip('"\'') for w in q.split() if w.strip()]
    return " ".join(f'"{w}"' for w in words if w)


# ---------------------------------------------------------------------------
# Route handlers
# ---------------------------------------------------------------------------

def _health(_params: dict) -> dict:
    row = _query_one("SELECT COUNT(*) AS n FROM papers")
    return {"status": "ok", "papers": row["n"] if row else 0}


def _search(params: dict) -> dict:
    q        = params.get("q", "").strip()
    limit    = min(int(params.get("limit", 20)), 100)
    year_min = params.get("year_min")
    year_max = params.get("year_max")

    if not q:
        return {"results": [], "total_found": 0}

    fts_q = _fts_escape(q)
    if not fts_q:
        return {"results": [], "total_found": 0}

    extra_where  = ""
    extra_params = []
    if year_min:
        extra_where += " AND p.year >= ?"
        extra_params.append(int(year_min))
    if year_max:
        extra_where += " AND p.year <= ?"
        extra_params.append(int(year_max))

    count_sql = (
        "SELECT COUNT(*) AS n "
        "FROM papers_fts "
        "JOIN papers p ON papers_fts.rowid = p.rowid "
        f"WHERE papers_fts MATCH ?{extra_where}"
    )
    total = (_query_one(count_sql, [fts_q] + extra_params) or {}).get("n", 0)

    select_sql = (
        "SELECT p.id, p.title, p.year, p.doi, p.cited_by_count, "
        "p.abstract, p.primary_topic, p.subfield "
        "FROM papers_fts "
        "JOIN papers p ON papers_fts.rowid = p.rowid "
        f"WHERE papers_fts MATCH ?{extra_where} "
        "ORDER BY rank "
        "LIMIT ?"
    )
    results = _query_all(select_sql, [fts_q] + extra_params + [limit])

    for r in results:
        if r.get("abstract"):
            r["abstract"] = r["abstract"][:400]

    return {"results": results, "total_found": total}


def _topic(params: dict) -> dict:
    name     = params.get("name", "").strip()
    sort     = params.get("sort", "citations")
    limit    = min(int(params.get("limit", 25)), 100)
    offset   = int(params.get("offset", 0))
    year_min = params.get("year_min")
    year_max = params.get("year_max")

    if not name:
        return {"results": [], "total": 0}

    order_clause = {
        "citations": "cited_by_count DESC",
        "year_desc": "year DESC, cited_by_count DESC",
        "year_asc":  "year ASC,  cited_by_count DESC",
    }.get(sort, "cited_by_count DESC")

    where        = "WHERE primary_topic = ?"
    where_params = [name]
    if year_min:
        where += " AND year >= ?"
        where_params.append(int(year_min))
    if year_max:
        where += " AND year <= ?"
        where_params.append(int(year_max))

    total = (_query_one(f"SELECT COUNT(*) AS n FROM papers {where}", where_params) or {}).get("n", 0)
    results = _query_all(
        f"SELECT id, title, year, doi, cited_by_count, abstract, primary_topic, subfield "
        f"FROM papers {where} ORDER BY {order_clause} LIMIT ? OFFSET ?",
        where_params + [limit, offset],
    )

    for r in results:
        if r.get("abstract"):
            r["abstract"] = r["abstract"][:300]

    return {"results": results, "total": total}


_taxonomy_cache: dict | None = None

def _taxonomy(_params: dict) -> dict:
    global _taxonomy_cache
    if _taxonomy_cache is not None:
        return _taxonomy_cache
    rows = _query_all("""
        SELECT subfield, primary_topic, COUNT(*) AS n
        FROM papers
        WHERE subfield != '' AND primary_topic != ''
        GROUP BY subfield, primary_topic
        ORDER BY subfield, n DESC
    """)
    subfields: dict[str, list] = {}
    for r in rows:
        sf = r["subfield"]
        subfields.setdefault(sf, []).append({"topic": r["primary_topic"], "count": r["n"]})
    _taxonomy_cache = {"subfields": subfields}
    return _taxonomy_cache


def _paper(params: dict) -> dict:
    pid = params.get("id", "").strip()
    if not pid:
        return {"error": "id required"}

    paper = _query_one("SELECT * FROM papers WHERE id = ?", [pid])
    if not paper:
        return {"error": "not found"}

    paper["authors"] = _query_all(
        "SELECT name, position FROM authors WHERE paper_id = ? "
        "ORDER BY CASE position WHEN 'first' THEN 0 WHEN 'last' THEN 2 ELSE 1 END",
        [pid],
    )
    paper["keywords"] = [
        r["keyword"] for r in _query_all(
            "SELECT keyword FROM keywords WHERE paper_id = ? LIMIT 20", [pid]
        )
    ]
    paper["topics"] = _query_all(
        "SELECT topic_name, is_primary FROM paper_topics "
        "WHERE paper_id = ? ORDER BY is_primary DESC",
        [pid],
    )
    return paper


def _bridges(params: dict) -> dict:
    name  = params.get("name", "").strip()
    limit = min(int(params.get("limit", 10)), 50)

    if not name:
        return {"results": []}

    results = _query_all(
        """
        SELECT p.id, p.title, p.year, p.cited_by_count,
               COUNT(DISTINCT pt.topic_name) AS cross_topic_count
        FROM (
            SELECT id, title, year, cited_by_count
            FROM papers
            WHERE primary_topic = ?
            ORDER BY cited_by_count DESC
            LIMIT 500
        ) p
        JOIN paper_topics pt ON pt.paper_id = p.id
        WHERE pt.topic_name != ?
        GROUP BY p.id
        HAVING cross_topic_count >= 2
        ORDER BY cross_topic_count DESC, p.cited_by_count DESC
        LIMIT ?
        """,
        [name, name, limit],
    )
    return {"results": results}


# ---------------------------------------------------------------------------
# HTTP handler
# ---------------------------------------------------------------------------

ROUTES = {
    "/health":   _health,
    "/search":   _search,
    "/topic":    _topic,
    "/paper":    _paper,
    "/bridges":  _bridges,
    "/taxonomy": _taxonomy,
}


class Handler(BaseHTTPRequestHandler):
    def do_OPTIONS(self):
        self.send_response(200)
        self._cors()
        self.end_headers()

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)
        params = dict(urllib.parse.parse_qsl(parsed.query))
        fn = ROUTES.get(parsed.path)
        if fn:
            try:
                self._json(fn(params))
            except Exception as exc:
                self._json({"error": str(exc)}, 500)
        else:
            self._json({"error": "not found"}, 404)

    def _cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def _json(self, data: dict, status: int = 200):
        body = json.dumps(data, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self._cors()
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass   # suppress per-request logs


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def _run_server(port: int) -> None:
    if not DB_PATH.exists():
        print(f"ERROR: DB not found at {DB_PATH}", file=sys.stderr)
        sys.exit(1)

    # Warm the connection
    _get_conn()
    n = (_query_one("SELECT COUNT(*) AS n FROM papers") or {}).get("n", 0)
    print(f"PaperBrain DB server  ->  http://localhost:{port}")
    print(f"  {n:,} papers loaded")
    print("  Press Ctrl+C to stop")

    # Build taxonomy cache in background so server starts immediately
    def _warm_taxonomy():
        _taxonomy({})
        print("  Taxonomy cache ready.", flush=True)
    threading.Thread(target=_warm_taxonomy, daemon=True, name="taxonomy-warmer").start()

    server = HTTPServer(("localhost", port), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nServer stopped.")


def _start_detached(port: int) -> None:
    """Launch server as detached background process, write PID to file."""
    import subprocess
    cmd = [sys.executable, __file__, "--port", str(port)]
    with open(LOG_FILE, "w") as lf:
        proc = subprocess.Popen(
            cmd,
            stdout=lf,
            stderr=subprocess.STDOUT,
            creationflags=0x00000008,   # DETACHED_PROCESS (Windows)
            close_fds=True,
        )
    PID_FILE.write_text(str(proc.pid))
    print(f"Server started  (PID {proc.pid})")
    print(f"  Logs: {LOG_FILE}")
    print(f"  Stop: python scripts/db_server.py --stop")


def _stop() -> None:
    if not PID_FILE.exists():
        print("No PID file found — server may not be running.")
        return
    pid = int(PID_FILE.read_text().strip())
    try:
        import signal
        os.kill(pid, signal.SIGTERM)
        PID_FILE.unlink(missing_ok=True)
        print(f"Sent SIGTERM to PID {pid}")
    except ProcessLookupError:
        print(f"PID {pid} not found — server already stopped.")
        PID_FILE.unlink(missing_ok=True)
    except Exception as e:
        print(f"Could not stop server: {e}")


def main() -> None:
    ap = argparse.ArgumentParser(description="PaperBrain local API server")
    ap.add_argument("--port",  type=int, default=DEFAULT_PORT)
    ap.add_argument("--start", action="store_true", help="launch as background process")
    ap.add_argument("--stop",  action="store_true", help="stop background process")
    args = ap.parse_args()

    if args.stop:
        _stop()
    elif args.start:
        _start_detached(args.port)
    else:
        _run_server(args.port)


if __name__ == "__main__":
    main()
