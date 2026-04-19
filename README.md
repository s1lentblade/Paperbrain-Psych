# PaperBrain-Psych

An end-to-end pipeline that fetches every Psychology paper indexed by
[OpenAlex](https://openalex.org) (~7M works), normalizes them into a local
SQLite database, and renders a browsable
[Obsidian](https://obsidian.md) vault of ~20,000 curated papers with topic
maps and concept graphs.

The generated vault lives at [`full psychology breakdown/`](./full%20psychology%20breakdown).

## Pipeline

```
fetch_papers.py  ──►  data/by_year/*.jsonl         (raw OpenAlex dump, ~29 GB)
      │
      ▼
build_db.py      ──►  data/papers.db               (SQLite w/ FTS, ~20 GB)
      │
      ▼
db_server.py     ──►  localhost query API
      │
      ▼
generate_vault.py ─►  full psychology breakdown/   (Obsidian vault)
generate_papers.py
generate_graphs.py
```

All of `data/` is `.gitignore`'d — it's multi-tens-of-GB and deterministically
reproducible from the scripts.

## ⚠️ OpenAlex API usage — paid accounts only

`fetch_papers.py` accepts a list of API keys via `--api-keys`. **Only use keys
from paid OpenAlex [Premium](https://openalex.org/pricing) accounts.**

OpenAlex's free "polite pool" is governed by rate limits and fair-use norms.
Rotating multiple free keys to circumvent those limits is abusive and
potentially a violation of the OpenAlex terms of service. Don't do it. Use a
single paid key, or several paid keys that you legitimately own, and set
`--workers` to something your quota can sustain.

## Requirements

- Python 3.10+
- `requests` (only hard dependency for `fetch_papers.py`)
- SQLite 3.35+ (for `build_db.py`)
- Obsidian (to browse the generated vault)

## Quickstart

```bash
# 1. Fetch (hours to days depending on quota)
python scripts/fetch_papers.py --api-keys $OPENALEX_KEY --workers 32

# 2. Build SQLite
python scripts/build_db.py

# 3. Start the query server
python scripts/db_server.py &

# 4. Generate the vault
python scripts/generate_vault.py
python scripts/generate_papers.py
python scripts/generate_graphs.py
```

## Scripts

| Script | Purpose |
|---|---|
| `fetch_papers.py` | Pull every Psychology work from OpenAlex into `data/by_year/` |
| `build_db.py` | Merge the JSONL dump into `data/papers.db` with FTS indexes |
| `db_server.py` | Local HTTP API over `papers.db` for the generators |
| `generate_vault.py` | Lay out the Obsidian vault skeleton (Topics, Maps) |
| `generate_papers.py` | Write the ~20 k paper notes |
| `generate_graphs.py` | Build concept / citation graphs |
| `patch_*.py`, `restore_*.py`, `trim_*.py`, `upgrade_*.py`, `relink_*.py` | One-off vault refinement passes |

## License

No license yet — treat as all-rights-reserved until one is added.
