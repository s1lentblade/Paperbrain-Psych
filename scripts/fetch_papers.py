"""
fetch_papers.py — Fetch ALL Psychology papers from OpenAlex (~7M works)

⚠️  API KEY POLICY — READ BEFORE USE  ⚠️
----------------------------------------------------------------------
Pass ONLY keys from paid OpenAlex Premium accounts via --api-keys.

OpenAlex's free "polite pool" is rate-limited and governed by fair-use
norms. Rotating multiple *free* keys to bypass those limits is abusive
and potentially violates the OpenAlex terms of service. Don't do it.

Use a single paid key, or several paid keys you legitimately own. Set
--workers to a level your quota can actually sustain.
----------------------------------------------------------------------

Usage:
    python scripts/fetch_papers.py --api-keys KEY1 KEY2 KEY3 KEY4
    python scripts/fetch_papers.py --api-keys KEY1 KEY2 --resume
    python scripts/fetch_papers.py --api-keys KEY1 KEY2 --workers 64
    python scripts/fetch_papers.py --api-keys KEY1 KEY2 --resume --fine-from 2020 --partition-days 7

Strategy:
    - Annual partitions by default (1 cursor chain per year)
    - Sub-year partitions for recent high-volume years via --fine-from + --partition-days
      e.g. --fine-from 2020 --partition-days 7  →  weekly chunks for 2020-2024 (260 units)
      This keeps all workers saturated even when few years remain.
    - Workers default to 32 (I/O-bound — threads sleep on network, not CPU)
    - Cursor-based pagination within each partition (gets every paper, not a sample)
    - Progress batched in memory, flushed to disk every 5 s — reduces lock
      contention 100x vs per-page disk writes; resume window is at most 5 s
    - Key rotation: when a key hits quota, silently rotates to the next key
      and keeps all workers running without dropping data
    - Only stops when ALL keys are exhausted

Output:
    data/by_year/papers_{YEAR}.jsonl  — one file per year (all partitions for a year append here)
    data/progress.json                — resume state (cursor + count per partition key)

Partition key format:
    Annual:    "2003"
    Sub-year:  "2020-01-01:2020-01-07"
"""

import argparse
import json
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

import requests

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_URL          = "https://api.openalex.org/works"
PSYCHOLOGY_FIELD  = "fields/32"
YEAR_START        = 1800
YEAR_END          = 2024
PER_PAGE          = 200

DATA_DIR          = Path(__file__).parent.parent / "data"
BY_YEAR_DIR       = DATA_DIR / "by_year"
PROGRESS_FILE     = DATA_DIR / "progress.json"

SELECT_FIELDS = ",".join([
    "id", "title", "authorships", "abstract_inverted_index",
    "publication_year", "doi", "cited_by_count",
    "primary_topic", "topics", "keywords", "type",
])

# ---------------------------------------------------------------------------
# Work unit
# ---------------------------------------------------------------------------

@dataclass
class WorkUnit:
    start: date
    end: date

    @property
    def key(self) -> str:
        """Progress dict key. Annual units use bare year string for backward compat."""
        if self.start.month == 1 and self.start.day == 1 \
                and self.end.month == 12 and self.end.day == 31 \
                and self.start.year == self.end.year:
            return str(self.start.year)
        return f"{self.start}:{self.end}"

    @property
    def filter_clause(self) -> str:
        # Whole-year units use the faster publication_year index
        if self.key == str(self.start.year):
            return f"primary_topic.field.id:{PSYCHOLOGY_FIELD},publication_year:{self.start.year}"
        return (f"primary_topic.field.id:{PSYCHOLOGY_FIELD},"
                f"from_publication_date:{self.start},to_publication_date:{self.end}")

    @property
    def year(self) -> int:
        return self.start.year

    @property
    def out_path(self) -> Path:
        return BY_YEAR_DIR / f"papers_{self.year}.jsonl"


def build_work_units(year_start: int, year_end: int,
                     fine_from: Optional[int], partition_days: int) -> list[WorkUnit]:
    """
    Build the full list of work units.
    Years < fine_from  →  one annual unit each.
    Years >= fine_from →  N-day chunks, staying within each calendar year.
    """
    units: list[WorkUnit] = []
    for year in range(year_start, year_end + 1):
        if fine_from and year >= fine_from:
            cur = date(year, 1, 1)
            year_end_date = date(year, 12, 31)
            while cur <= year_end_date:
                chunk_end = min(cur + timedelta(days=partition_days - 1), year_end_date)
                units.append(WorkUnit(cur, chunk_end))
                cur = chunk_end + timedelta(days=1)
        else:
            units.append(WorkUnit(date(year, 1, 1), date(year, 12, 31)))
    return units


# ---------------------------------------------------------------------------
# Shared state
# ---------------------------------------------------------------------------

progress_lock = threading.Lock()
file_locks: dict[int, threading.Lock] = {}   # keyed by year — multiple monthly units share one file
print_lock = threading.Lock()

# Key rotation state
_key_lock = threading.Lock()
_api_keys: list[str] = []
_current_key_idx: int = 0
_spent_keys: list[str] = []
all_keys_exhausted = threading.Event()

# Progress write buffer — workers write here; background flusher persists to disk
_progress_buffer: dict[str, dict] = {}
_buffer_lock = threading.Lock()
FLUSH_INTERVAL = 5.0  # seconds between disk writes


def log(msg: str) -> None:
    with print_lock:
        print(msg, flush=True)


# ---------------------------------------------------------------------------
# Key rotation helpers
# ---------------------------------------------------------------------------

def get_current_key() -> str | None:
    with _key_lock:
        if _current_key_idx >= len(_api_keys):
            return None
        return _api_keys[_current_key_idx]


def rotate_key(exhausted_key: str) -> str | None:
    """
    Mark exhausted_key as spent and advance to the next key.
    Returns the new active key, or None if all keys are spent.
    Thread-safe — only the first thread to report a given key logs and rotates;
    subsequent threads see the already-rotated key and return silently.
    """
    global _current_key_idx
    with _key_lock:
        if _current_key_idx < len(_api_keys) and _api_keys[_current_key_idx] != exhausted_key:
            # Another thread already rotated past this key — return silently
            return _api_keys[_current_key_idx]

        # We are the rotating thread
        log(f"\n  [QUOTA] Key ending ...{exhausted_key[-6:]} hit daily limit. Rotating...")
        _spent_keys.append(exhausted_key)
        _current_key_idx += 1

        if _current_key_idx >= len(_api_keys):
            all_keys_exhausted.set()
            log("\n" + "=" * 60)
            log("  ALL API KEYS EXHAUSTED")
            log(f"  Used {len(_spent_keys)} key(s). No data has been lost.")
            log("")
            log("  To continue, add more keys and re-run with --resume:")
            log("  python scripts/fetch_papers.py --api-keys KEY1 KEY2 ... --resume")
            log("=" * 60 + "\n")
            return None

        new_key = _api_keys[_current_key_idx]
        remaining_keys = len(_api_keys) - _current_key_idx
        log(f"  [KEY ROTATION] Key {_current_key_idx}/{len(_api_keys)} now active "
            f"(ending ...{new_key[-6:]})  |  {remaining_keys} key(s) remaining\n")
        return new_key


# ---------------------------------------------------------------------------
# Progress file helpers
# ---------------------------------------------------------------------------

def load_progress() -> dict:
    if PROGRESS_FILE.exists():
        try:
            return json.loads(PROGRESS_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    return {}


def save_progress(progress: dict) -> None:
    PROGRESS_FILE.write_text(json.dumps(progress, indent=2), encoding="utf-8")


def update_unit_progress(key: str, cursor: str | None, fetched: int, done: bool) -> None:
    """Write to the in-memory buffer only. The flusher persists to disk every 5 s."""
    with _buffer_lock:
        _progress_buffer[key] = {"cursor": cursor, "fetched": fetched, "done": done}


def flush_progress_buffer() -> None:
    """Merge buffered updates into progress.json atomically. Safe to call from any thread."""
    with _buffer_lock:
        if not _progress_buffer:
            return
        snapshot = dict(_progress_buffer)
        _progress_buffer.clear()

    with progress_lock:
        on_disk = load_progress()
        on_disk.update(snapshot)
        save_progress(on_disk)


def start_progress_flusher() -> threading.Thread:
    """Launch a daemon thread that flushes the progress buffer every FLUSH_INTERVAL seconds."""
    def _run() -> None:
        while not all_keys_exhausted.is_set():
            time.sleep(FLUSH_INTERVAL)
            flush_progress_buffer()
        flush_progress_buffer()  # final flush on exit

    t = threading.Thread(target=_run, daemon=True, name="progress-flusher")
    t.start()
    return t


# ---------------------------------------------------------------------------
# Abstract reconstruction
# ---------------------------------------------------------------------------

def reconstruct_abstract(inv: dict | None) -> str:
    if not inv:
        return ""
    positions: dict[int, str] = {}
    for word, pos_list in inv.items():
        for pos in pos_list:
            positions[pos] = word
    return " ".join(positions[i] for i in sorted(positions))


# ---------------------------------------------------------------------------
# API fetch — single page with retry, rate-limit backoff, and key rotation
# ---------------------------------------------------------------------------

QUOTA_PHRASES = (
    "insufficient", "quota", "daily limit", "credits",
    "payment", "upgrade", "exceeded your", "over your limit",
)


def is_quota_error(resp: requests.Response) -> bool:
    if resp.status_code == 402:
        return True
    if resp.status_code == 429:
        try:
            msg = str(resp.json()).lower()
        except Exception:
            msg = resp.text.lower()
        return any(phrase in msg for phrase in QUOTA_PHRASES)
    return False


def fetch_page(params: dict, max_attempts: int = 8) -> dict | None:
    """
    Fetch one page. Handles:
      - Rate limit 429  -> exponential backoff, retry
      - Quota error     -> rotate to next key, retry transparently
      - All keys spent  -> return None (workers will stop)
      - Network errors  -> retry with backoff
    """
    attempt = 0
    while attempt < max_attempts:
        if all_keys_exhausted.is_set():
            return None

        key = get_current_key()
        if not key:
            return None

        try:
            resp = requests.get(BASE_URL, params={**params, "api_key": key}, timeout=30)

            if resp.status_code == 401:
                new_key = rotate_key(key)  # logs inside rotate_key
                if not new_key:
                    return None
                attempt = 0
                continue

            if is_quota_error(resp):
                new_key = rotate_key(key)  # logs inside rotate_key
                if not new_key:
                    return None
                attempt = 0
                continue

            if resp.status_code == 429:
                wait = min(2 ** attempt * 2, 60)
                log(f"    [rate limit] waiting {wait}s ...")
                time.sleep(wait)
                attempt += 1
                continue

            resp.raise_for_status()
            return resp.json()

        except requests.RequestException as exc:
            if attempt >= max_attempts - 1:
                log(f"    [error] {exc} -- giving up")
                return {}
            time.sleep(2 ** attempt)
            attempt += 1

    return {}


# ---------------------------------------------------------------------------
# Per-unit worker
# ---------------------------------------------------------------------------

def fetch_unit(unit: WorkUnit) -> tuple[int, str]:
    """Fetch all papers for one work unit. Returns (count, status)."""
    if all_keys_exhausted.is_set():
        return 0, "paused"

    with progress_lock:
        progress = load_progress()

    state = progress.get(unit.key, {})
    if state.get("done"):
        return state.get("fetched", 0), "already_done"

    cursor  = state.get("cursor") or "*"
    fetched = state.get("fetched") or 0
    lock    = file_locks[unit.year]

    params = {
        "filter":   unit.filter_clause,
        "select":   SELECT_FIELDS,
        "per_page": PER_PAGE,
        "mailto":   "paperbrain@local",
    }

    out_f = open(unit.out_path, "a", encoding="utf-8")
    try:
        while True:
            if all_keys_exhausted.is_set():
                update_unit_progress(unit.key, cursor, fetched, done=False)
                return fetched, "paused"

            params["cursor"] = cursor
            data = fetch_page(params)

            if data is None:
                update_unit_progress(unit.key, cursor, fetched, done=False)
                return fetched, "paused"

            results = data.get("results") or []
            if not results:
                break

            lines = []
            for work in results:
                work["abstract"] = reconstruct_abstract(work.pop("abstract_inverted_index", None))
                lines.append(json.dumps(work, ensure_ascii=False))

            with lock:
                out_f.write("\n".join(lines) + "\n")
                out_f.flush()
            fetched += len(lines)

            next_cursor = data.get("meta", {}).get("next_cursor")
            update_unit_progress(unit.key, next_cursor, fetched, done=False)

            if not next_cursor:
                break
            cursor = next_cursor
            time.sleep(0.05)

    finally:
        out_f.close()

    update_unit_progress(unit.key, None, fetched, done=True)
    return fetched, "done"


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def count_total_fetched(progress: dict) -> int:
    return sum(v.get("fetched", 0) for v in progress.values())


def main() -> None:
    global _api_keys, _current_key_idx

    cpu_cores = os.cpu_count() or 4
    default_workers = max(32, cpu_cores * 2)

    parser = argparse.ArgumentParser(description="Fetch all ~7M Psychology papers from OpenAlex")
    parser.add_argument("--api-keys", nargs="+", required=True,
                        help="One or more OpenAlex API keys (rotated automatically on quota exhaustion)")
    parser.add_argument("--workers", type=int, default=default_workers,
                        help=f"Parallel workers (default: {default_workers}; I/O-bound so more than CPU count is fine)")
    parser.add_argument("--resume",   action="store_true", help="Resume from saved progress")
    parser.add_argument("--year-start", type=int, default=YEAR_START)
    parser.add_argument("--year-end",   type=int, default=YEAR_END)
    parser.add_argument("--fine-from", type=int, default=None,
                        help="Switch to sub-year partitioning for years >= this value (e.g. 2020)")
    parser.add_argument("--partition-days", type=int, default=7,
                        help="Partition size in days for sub-year mode (default: 7 = weekly)")
    args = parser.parse_args()

    _api_keys = args.api_keys
    _current_key_idx = 0

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    BY_YEAR_DIR.mkdir(parents=True, exist_ok=True)

    all_units = build_work_units(
        args.year_start, args.year_end,
        fine_from=args.fine_from,
        partition_days=args.partition_days,
    )

    # One file lock per year — multiple sub-year partitions share the same output file
    for unit in all_units:
        if unit.year not in file_locks:
            file_locks[unit.year] = threading.Lock()

    progress = load_progress()
    already_done = [u for u in all_units if progress.get(u.key, {}).get("done")]
    remaining    = [u for u in all_units if not progress.get(u.key, {}).get("done")]
    total_fetched = count_total_fetched(progress)

    # Stats
    unique_years = len(set(u.year for u in all_units))
    done_years   = len(set(u.year for u in already_done))

    print(f"OpenAlex Psychology full pull (~7M papers)")
    print(f"Years: {args.year_start}-{args.year_end}  |  Workers: {args.workers}  |  Keys: {len(_api_keys)}")
    print(f"Partitions: {len(all_units)} total  "
          f"({'annual' if not args.fine_from else f'annual to {args.fine_from-1}, then {args.partition_days}-day chunks'})")
    print(f"Progress: {len(already_done)}/{len(all_units)} partitions done "
          f"({done_years}/{unique_years} years), {total_fetched:,} papers on disk")
    if not remaining:
        print("All partitions complete!")
        return
    print(f"Remaining: {len(remaining)} partitions  |  Active key: ...{_api_keys[0][-6:]}")
    print()

    fetched_this_run = 0
    start_progress_flusher()

    with ThreadPoolExecutor(max_workers=args.workers) as executor:
        futures = {executor.submit(fetch_unit, unit): unit for unit in remaining}
        try:
            for future in as_completed(futures):
                unit = futures[future]
                count, status = future.result()
                fetched_this_run += count

                if status == "done":
                    total_so_far = count_total_fetched(load_progress())
                    log(f"  {unit.key}: {count:>6,} papers  [done]  | total {total_so_far:>8,}")
                elif status == "paused":
                    log(f"  {unit.key}: paused at {count:,} papers")

                if all_keys_exhausted.is_set():
                    for f in futures:
                        f.cancel()
                    break

        except KeyboardInterrupt:
            log("\nInterrupted -- saving progress...")
            all_keys_exhausted.set()
            for f in futures:
                f.cancel()

    flush_progress_buffer()  # drain anything remaining in the buffer

    final_progress = load_progress()
    total_papers   = count_total_fetched(final_progress)
    done_partitions = sum(1 for v in final_progress.values() if v.get("done"))

    print()
    print("=" * 50)
    if all_keys_exhausted.is_set():
        print(f"PAUSED -- all keys exhausted or interrupted")
        print(f"  Papers this run:   {fetched_this_run:,}")
        print(f"  Total on disk:     {total_papers:,}")
        print(f"  Partitions done:   {done_partitions}/{len(all_units)}")
        print()
        print("  Resume with fresh keys:")
        print("  python scripts/fetch_papers.py --api-keys KEY1 KEY2 ... --resume")
    else:
        print(f"COMPLETE")
        print(f"  Total papers:      {total_papers:,}")
        print(f"  Partitions done:   {done_partitions}/{len(all_units)}")
    print("=" * 50)


if __name__ == "__main__":
    main()
