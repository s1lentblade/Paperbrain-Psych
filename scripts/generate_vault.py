"""
generate_vault.py — Build an Obsidian topic-landscape vault from 6.6M Psychology papers

Usage:
    python scripts/generate_vault.py

Input:  data/by_year/papers_*.jsonl
Output: vault/

Design:
    - ONE streaming pass through all papers — no RAM blow-up
    - Collects per-topic: paper count, year trend, top-5 cited, top-3 authors,
      and pairwise co-occurrence with other topics
    - Vault nodes: ~30 subfield MOCs + ~144 topic notes + ~720 exemplar paper notes
    - Topic notes link directly to co-occurring topics (lateral edges in graph)
    - No per-paper notes — the graph shows the topic landscape, not a paper dump
"""

import json
import heapq
import os
import re
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path

DATA_DIR    = Path(__file__).parent.parent / "data"
BY_YEAR_DIR = DATA_DIR / "by_year"
VAULT_DIR   = Path(__file__).parent.parent / "vault"

EXEMPLARS_PER_TOPIC  = 5    # top-cited paper notes per topic
COOCCUR_LINKS        = 6    # lateral topic-to-topic wikilinks per topic note
COOCCUR_MIN_COUNT    = 20   # minimum co-occurrences to create a link
TOP_AUTHORS          = 3    # authors listed per topic

# ---------------------------------------------------------------------------
# Colour palette
# ---------------------------------------------------------------------------

SUBFIELD_HUES: dict[str, float] = {
    "Social Psychology":                            0.0,
    "Clinical Psychology":                          210.0,
    "Developmental and Educational Psychology":     120.0,
    "Experimental and Cognitive Psychology":        270.0,
    "Neuropsychology and Physiological Psychology": 30.0,
    "Applied Psychology":                           170.0,
    "General Psychology":                           52.0,
}
DEFAULT_HUE = 300.0


def hsl_to_rgb_int(h: float, s: float, l: float) -> int:
    c = (1 - abs(2 * l - 1)) * s
    x = c * (1 - abs((h / 60) % 2 - 1))
    m = l - c / 2
    if   h < 60:  r, g, b = c, x, 0
    elif h < 120: r, g, b = x, c, 0
    elif h < 180: r, g, b = 0, c, x
    elif h < 240: r, g, b = 0, x, c
    elif h < 300: r, g, b = x, 0, c
    else:         r, g, b = c, 0, x
    ri, gi, bi = int((r + m) * 255), int((g + m) * 255), int((b + m) * 255)
    return (ri << 16) | (gi << 8) | bi


def build_color_map(subfield_topics: dict) -> tuple[dict, dict, dict]:
    moc_colors: dict[str, int] = {}
    topic_colors: dict[str, int] = {}
    exemplar_colors: dict[str, int] = {}

    for subfield, topics in subfield_topics.items():
        hue = SUBFIELD_HUES.get(subfield, DEFAULT_HUE)
        moc_colors[subfield]     = hsl_to_rgb_int(hue, 0.90, 0.45)
        exemplar_colors[subfield] = hsl_to_rgb_int(hue, 0.40, 0.72)

        topic_list = sorted(topics.keys())
        n = len(topic_list)
        for i, topic in enumerate(topic_list):
            lightness = 0.35 + (i / max(n - 1, 1)) * 0.27
            topic_colors[topic] = hsl_to_rgb_int(hue, 0.75, lightness)

    return moc_colors, topic_colors, exemplar_colors


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def subfield_to_tag(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

def topic_to_tag(name: str) -> str:
    return re.sub(r"[^a-z0-9]+", "-", name.lower()).strip("-")

def sanitize_filename(text: str, max_len: int = 100) -> str:
    if not text:
        return "Untitled"
    text = re.sub(r'[\\/*?:"<>|#^[\]{}]', "", text)
    text = re.sub(r"\s+", " ", text).strip()
    if len(text) > max_len:
        text = text[:max_len].rsplit(" ", 1)[0]
    return text or "Untitled"

def safe_wikilink(name: str) -> str:
    return name.replace("|", "-").replace("[", "(").replace("]", ")")

def yaml_str(value: str) -> str:
    return '"' + value.replace('"', '\\"') + '"'

def yaml_list(items: list) -> str:
    if not items:
        return "[]"
    escaped = ['"' + str(i).replace('"', '\\"') + '"' for i in items]
    return "[" + ", ".join(escaped) + "]"


# ---------------------------------------------------------------------------
# Parallel data collection
# ---------------------------------------------------------------------------

def _assign_files(files_with_sizes: list, n_workers: int) -> list[list[str]]:
    """
    Greedy bin-packing: sort files by size descending, assign each to the
    worker with the least accumulated bytes so far. Keeps load balanced even
    when a few files are 100x larger than the rest.
    """
    buckets      = [[] for _ in range(n_workers)]
    bucket_bytes = [0]  * n_workers
    for path, size in sorted(files_with_sizes, key=lambda x: -x[1]):
        idx = bucket_bytes.index(min(bucket_bytes))
        buckets[idx].append(str(path))
        bucket_bytes[idx] += size
    total_gb = sum(s for _, s in files_with_sizes) / 1024**3
    print(f"  Load balance across {n_workers} workers "
          f"(total {total_gb:.1f} GB):", flush=True)
    for i, (b, sz) in enumerate(zip(buckets, bucket_bytes)):
        print(f"    worker {i}: {len(b)} files, {sz/1024**3:.2f} GB", flush=True)
    return buckets


def _process_files(file_paths: list[str]) -> dict:
    """
    Worker function (runs in a separate process).
    Processes a list of year-file paths and returns plain-dict partial stats.
    No defaultdicts — must be picklable.
    """
    subfield_topics   = {}   # sf -> { topic -> count }
    topic_to_subfield = {}
    topic_year_counts = {}   # topic -> { year -> count }
    topic_top_papers  = {}   # topic -> list of (citations, counter, slim_dict)
    topic_authors     = {}   # topic -> { author -> count }
    cooccurrence      = {}   # (topic_a, topic_b) -> count
    _ctr              = 0    # unique tiebreaker so heapq never compares dicts

    for file_path in file_paths:
        with open(file_path, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    paper = json.loads(line)
                except json.JSONDecodeError:
                    continue

                pt            = paper.get("primary_topic") or {}
                primary_topic = (pt.get("display_name") or "").strip()
                sf_obj        = pt.get("subfield") or {}
                subfield      = (sf_obj.get("display_name") or "Unknown Subfield").strip()
                year          = paper.get("publication_year")
                citations     = paper.get("cited_by_count") or 0

                if not primary_topic:
                    continue

                # Taxonomy
                if subfield not in subfield_topics:
                    subfield_topics[subfield] = {}
                subfield_topics[subfield][primary_topic] = \
                    subfield_topics[subfield].get(primary_topic, 0) + 1
                topic_to_subfield[primary_topic] = subfield

                # Year counts
                if primary_topic not in topic_year_counts:
                    topic_year_counts[primary_topic] = {}
                if year:
                    topic_year_counts[primary_topic][year] = \
                        topic_year_counts[primary_topic].get(year, 0) + 1

                # Top-cited exemplars (min-heap per topic)
                slim = {
                    "title":         (paper.get("title") or "Untitled").strip(),
                    "year":          year,
                    "doi":           paper.get("doi") or "",
                    "cited_by_count": citations,
                    "authorships":   (paper.get("authorships") or [])[:3],
                    "abstract":      (paper.get("abstract") or "")[:800],
                    "primary_topic": primary_topic,
                    "subfield":      subfield,
                }
                heap  = topic_top_papers.setdefault(primary_topic, [])
                _ctr += 1
                entry = (citations, _ctr, slim)
                if len(heap) < EXEMPLARS_PER_TOPIC:
                    heapq.heappush(heap, entry)
                elif citations > heap[0][0]:
                    heapq.heapreplace(heap, entry)

                # Top authors
                auth_dict = topic_authors.setdefault(primary_topic, {})
                for auth in (paper.get("authorships") or [])[:5]:
                    name = ((auth.get("author") or {}).get("display_name") or "").strip()
                    if name:
                        auth_dict[name] = auth_dict.get(name, 0) + 1

                # Co-occurrence — collect all topic names on this paper,
                # no filtering here (filter to known topics after merge)
                all_topics = [primary_topic]
                for t in (paper.get("topics") or []):
                    tname = (t.get("display_name") or "").strip()
                    if tname and tname != primary_topic:
                        all_topics.append(tname)

                for i in range(len(all_topics)):
                    for j in range(i + 1, len(all_topics)):
                        a, b = all_topics[i], all_topics[j]
                        key  = (min(a, b), max(a, b))
                        cooccurrence[key] = cooccurrence.get(key, 0) + 1

    return {
        "subfield_topics":   subfield_topics,
        "topic_to_subfield": topic_to_subfield,
        "topic_year_counts": topic_year_counts,
        "topic_top_papers":  topic_top_papers,
        "topic_authors":     topic_authors,
        "cooccurrence":      cooccurrence,
    }


def _merge(partials: list[dict]) -> dict:
    """Merge partial dicts from all workers into one unified stats dict."""
    merged_sf     = {}
    merged_t2sf   = {}
    merged_yc     = {}
    merged_papers = {}
    merged_auth   = {}
    merged_cooc   = {}

    for p in partials:
        # subfield_topics
        for sf, topics in p["subfield_topics"].items():
            sf_d = merged_sf.setdefault(sf, {})
            for t, c in topics.items():
                sf_d[t] = sf_d.get(t, 0) + c

        # topic_to_subfield (same value from all workers, just union)
        merged_t2sf.update(p["topic_to_subfield"])

        # topic_year_counts
        for t, yc in p["topic_year_counts"].items():
            td = merged_yc.setdefault(t, {})
            for y, c in yc.items():
                td[y] = td.get(y, 0) + c

        # topic_top_papers — extend then trim to EXEMPLARS_PER_TOPIC
        for t, heap in p["topic_top_papers"].items():
            merged_papers.setdefault(t, []).extend(heap)

        # topic_authors
        for t, authors in p["topic_authors"].items():
            td = merged_auth.setdefault(t, {})
            for a, c in authors.items():
                td[a] = td.get(a, 0) + c

        # cooccurrence
        for key, c in p["cooccurrence"].items():
            merged_cooc[key] = merged_cooc.get(key, 0) + c

    # Trim exemplar heaps and filter co-occurrence to known psychology topics
    known_topics = set(merged_t2sf.keys())
    for t in merged_papers:
        merged_papers[t] = heapq.nlargest(
            EXEMPLARS_PER_TOPIC, merged_papers[t], key=lambda x: x[0]
        )
    merged_cooc = {
        (a, b): c for (a, b), c in merged_cooc.items()
        if a in known_topics and b in known_topics
    }

    return {
        "subfield_topics":   merged_sf,
        "topic_to_subfield": merged_t2sf,
        "topic_year_counts": merged_yc,
        "topic_top_papers":  merged_papers,
        "topic_authors":     merged_auth,
        "cooccurrence":      merged_cooc,
    }


def collect_stats(by_year_dir: Path) -> dict:
    year_files = sorted(by_year_dir.glob("papers_*.jsonl"))
    files_with_sizes = [(f, f.stat().st_size) for f in year_files]

    n_workers = max(2, os.cpu_count() or 4)
    buckets   = _assign_files(files_with_sizes, n_workers)
    # Drop empty buckets (fewer files than workers)
    buckets   = [b for b in buckets if b]

    print(f"  Launching {len(buckets)} worker processes...", flush=True)
    partials = []
    with ProcessPoolExecutor(max_workers=len(buckets)) as pool:
        futures = {pool.submit(_process_files, bucket): i
                   for i, bucket in enumerate(buckets)}
        for future in as_completed(futures):
            idx = futures[future]
            try:
                partials.append(future.result())
                print(f"  Worker {idx} done.", flush=True)
            except Exception as exc:
                print(f"  Worker {idx} FAILED: {exc}", flush=True)
                raise

    print("  Merging results...", flush=True)
    return _merge(partials)


# ---------------------------------------------------------------------------
# Trend helper
# ---------------------------------------------------------------------------

def compute_trend(year_counts: dict) -> str:
    """Return 'growing', 'declining', or 'stable' based on recent vs older paper counts."""
    if not year_counts:
        return "unknown"
    years = sorted(year_counts.keys())
    recent_years  = [y for y in years if y >= 2020]
    earlier_years = [y for y in years if 2015 <= y < 2020]
    recent  = sum(year_counts[y] for y in recent_years)
    earlier = sum(year_counts[y] for y in earlier_years)
    if not earlier:
        return "emerging"
    ratio = recent / earlier
    if ratio > 1.25:
        return "growing"
    if ratio < 0.75:
        return "declining"
    return "stable"


# ---------------------------------------------------------------------------
# Vault writers
# ---------------------------------------------------------------------------

def write_overview_moc(stats: dict) -> None:
    subfield_topics = stats["subfield_topics"]
    path = VAULT_DIR / "Maps" / "Psychology Overview.md"
    path.parent.mkdir(parents=True, exist_ok=True)

    total_papers = sum(sum(t.values()) for t in subfield_topics.values())
    lines = [
        "---",
        'title: "Psychology — Overview"',
        'type: "overview-moc"',
        f"subfield_count: {len(subfield_topics)}",
        f"total_papers: {total_papers}",
        "---", "",
        "# Psychology — Overview", "",
        "Topic landscape built from **{:,} psychology papers** (OpenAlex full corpus).".format(total_papers),
        "Each subfield links to its topic breakdown.",
        "",
        "## Subfields", "",
    ]
    for sf, topics in sorted(subfield_topics.items(), key=lambda x: -sum(x[1].values())):
        total = sum(topics.values())
        lines.append(f"- [[{safe_wikilink(sf)}]] — {total:,} papers, {len(topics)} topics")

    path.write_text("\n".join(lines), encoding="utf-8")


def write_subfield_mocs(stats: dict) -> None:
    subfield_topics = stats["subfield_topics"]
    moc_dir = VAULT_DIR / "Maps"
    moc_dir.mkdir(parents=True, exist_ok=True)

    for subfield, topics in subfield_topics.items():
        path = moc_dir / (sanitize_filename(subfield) + ".md")
        total = sum(topics.values())
        sf_tag = subfield_to_tag(subfield)
        lines = [
            "---",
            f"title: {yaml_str(subfield)}",
            'type: "subfield-moc"',
            f"tags: [moc, {sf_tag}]",
            f"paper_count: {total}",
            f"topic_count: {len(topics)}",
            "---", "",
            f"# {subfield}", "",
            f"**{total:,} papers** across **{len(topics)} topics**.",
            "",
            "← [[Psychology Overview]]",
            "",
            "## Topics", "",
        ]
        for topic, count in sorted(topics.items(), key=lambda x: -x[1]):
            lines.append(f"- [[{safe_wikilink(topic)}]] — {count:,} papers")

        path.write_text("\n".join(lines), encoding="utf-8")


def write_topic_notes(stats: dict) -> None:
    subfield_topics   = stats["subfield_topics"]
    topic_to_subfield = stats["topic_to_subfield"]
    topic_year_counts = stats["topic_year_counts"]
    topic_top_papers  = stats["topic_top_papers"]
    topic_authors     = stats["topic_authors"]
    cooccurrence      = stats["cooccurrence"]

    topics_dir = VAULT_DIR / "Topics"
    topics_dir.mkdir(parents=True, exist_ok=True)

    # Pre-compute per-topic co-occurrence lookup: topic -> sorted [(count, other_topic)]
    topic_cooccur: dict[str, list] = defaultdict(list)
    for (a, b), count in cooccurrence.items():
        if count >= COOCCUR_MIN_COUNT:
            topic_cooccur[a].append((count, b))
            topic_cooccur[b].append((count, a))
    for t in topic_cooccur:
        topic_cooccur[t].sort(reverse=True)

    for subfield, topics in subfield_topics.items():
        for topic, count in topics.items():
            path = topics_dir / (sanitize_filename(topic) + ".md")
            sf_tag  = subfield_to_tag(subfield)
            t_tag   = topic_to_tag(topic)
            trend   = compute_trend(topic_year_counts.get(topic, {}))

            # Top authors
            authors_raw = topic_authors.get(topic, {})
            top_authors = sorted(authors_raw.items(), key=lambda x: -x[1])[:TOP_AUTHORS]

            # Top cited exemplars (sorted descending)
            exemplars = sorted(topic_top_papers.get(topic, []), key=lambda x: -x[0])

            # Co-occurring topics (exclude same subfield to keep lateral links cross-cluster)
            related = topic_cooccur.get(topic, [])[:COOCCUR_LINKS]

            lines = [
                "---",
                f"title: {yaml_str(topic)}",
                'type: "topic"',
                f"tags: [topic, {sf_tag}, {t_tag}]",
                f"subfield: {yaml_str(subfield)}",
                f"paper_count: {count}",
                f"trend: {yaml_str(trend)}",
                "---", "",
                f"# {topic}", "",
                f"**Subfield:** [[{safe_wikilink(subfield)}]]  ",
                f"**Papers:** {count:,}  ",
                f"**Trend:** {trend}",
                "",
            ]

            if top_authors:
                author_str = ", ".join(f"{a} ({n})" for a, n in top_authors)
                lines += [f"**Top authors:** {author_str}", ""]

            if related:
                lines += ["## Related topics", ""]
                for cnt, other in related:
                    lines.append(f"- [[{safe_wikilink(other)}]] — {cnt:,} co-occurrences")
                lines.append("")

            if exemplars:
                lines += ["## Landmark papers", ""]
                for entry in exemplars:
                    p       = entry[-1]   # (citations, counter, slim_dict)
                    p_title = p.get("title", "Untitled")
                    p_year  = p.get("year", "")
                    p_cite  = p.get("cited_by_count", 0)
                    fname   = sanitize_filename(p_title)
                    if p_year:
                        fname = f"{fname} ({p_year})"
                    lines.append(f"- [[{safe_wikilink(fname)}]] — {p_cite:,} citations")
                lines.append("")

            # Year trend sparkline (paper counts 2010-2024)
            year_counts = topic_year_counts.get(topic, {})
            spark_years = list(range(2010, 2025))
            spark_vals  = [year_counts.get(y, 0) for y in spark_years]
            if any(spark_vals):
                lines += ["## Publication trend (2010–2024)", ""]
                mx = max(spark_vals) or 1
                for yr, val in zip(spark_years, spark_vals):
                    bar_len = int(val / mx * 20)
                    lines.append(f"`{yr}` {'█' * bar_len} {val:,}")
                lines.append("")

            path.write_text("\n".join(lines), encoding="utf-8")


def write_exemplar_notes(stats: dict) -> int:
    """Write top-cited paper notes (one per exemplar, linked to their topic)."""
    topic_top_papers  = stats["topic_top_papers"]
    topic_to_subfield = stats["topic_to_subfield"]

    papers_dir = VAULT_DIR / "Papers"
    papers_dir.mkdir(parents=True, exist_ok=True)

    seen_filenames: dict[str, int] = {}
    written = 0

    for topic, heap in topic_top_papers.items():
        subfield = topic_to_subfield.get(topic, "")
        sf_tag   = subfield_to_tag(subfield) if subfield else "unknown"

        for entry in sorted(heap, key=lambda x: -x[0]):
            p = entry[-1]   # (citations, counter, slim_dict)
            title     = p.get("title", "Untitled")
            year      = p.get("year", "")
            doi       = p.get("doi", "")
            citations = p.get("cited_by_count", 0)
            abstract  = p.get("abstract", "")

            authors = []
            for a in (p.get("authorships") or []):
                name = ((a.get("author") or {}).get("display_name") or "").strip()
                if name:
                    authors.append(name)

            base = sanitize_filename(title)
            if year:
                base = f"{base} ({year})"
            if base in seen_filenames:
                seen_filenames[base] += 1
                filename = f"{base} [{seen_filenames[base]}].md"
            else:
                seen_filenames[base] = 0
                filename = f"{base}.md"

            path = papers_dir / filename

            lines = [
                "---",
                f"title: {yaml_str(title)}",
                f"authors: {yaml_list(authors)}",
                f"tags: [exemplar, {sf_tag}]",
            ]
            if year:  lines.append(f"year: {year}")
            if doi:   lines.append(f"doi: {yaml_str(doi)}")
            lines.append(f"citations: {citations}")
            lines.append(f"topic: {yaml_str(topic)}")
            if subfield: lines.append(f"subfield: {yaml_str(subfield)}")
            lines += ["---", "", f"# {title}", ""]

            if authors: lines.append(f"**Authors:** {', '.join(authors)}")
            if year:    lines.append(f"**Year:** {year}")
            lines.append(f"**Citations:** {citations:,}")
            if doi:     lines.append(f"**DOI:** {doi}")
            lines.append("")

            if abstract:
                lines += ["## Abstract", "", abstract, ""]

            lines += [
                "## Topic", "",
                f"- [[{safe_wikilink(topic)}]]",
                f"- [[{safe_wikilink(subfield)}]]",
            ]

            path.write_text("\n".join(lines), encoding="utf-8")
            written += 1

    return written


def write_obsidian_config(moc_colors: dict, topic_colors: dict, exemplar_colors: dict) -> None:
    obsidian_dir = VAULT_DIR / ".obsidian"
    obsidian_dir.mkdir(parents=True, exist_ok=True)

    (obsidian_dir / "app.json").write_text(
        json.dumps({"legacyEditor": False, "livePreview": True, "defaultViewMode": "preview"}, indent=2),
        encoding="utf-8",
    )

    color_groups = []

    for subfield, rgb in moc_colors.items():
        color_groups.append({"query": f"tag:moc tag:{subfield_to_tag(subfield)}",
                              "color": {"a": 1, "rgb": rgb}})

    for topic, rgb in topic_colors.items():
        color_groups.append({"query": f"tag:topic tag:{topic_to_tag(topic)}",
                              "color": {"a": 1, "rgb": rgb}})

    for subfield, rgb in exemplar_colors.items():
        color_groups.append({"query": f"tag:exemplar tag:{subfield_to_tag(subfield)}",
                              "color": {"a": 1, "rgb": rgb}})

    graph_config = {
        "collapse-filter": False,
        "search": "",
        "showTags": False,
        "showAttachments": False,
        "hideUnresolved": False,
        "showOrphans": True,
        "collapse-color-groups": False,
        "colorGroups": color_groups,
        "collapse-display": False,
        "showArrow": False,
        "textFadeMultiplier": 0,
        "nodeSizeMultiplier": 1.5,
        "lineSizeMultiplier": 0.4,
        "centerStrength": 0.3,
        "repelStrength": 10,
        "linkStrength": 1.0,
        "linkDistance": 40,
        "scale": 1,
        "close": False,
    }
    (obsidian_dir / "graph.json").write_text(json.dumps(graph_config, indent=2), encoding="utf-8")
    print(f"  Color groups: {len(color_groups)} "
          f"({len(moc_colors)} MOCs + {len(topic_colors)} topics + {len(exemplar_colors)} exemplar subfields)")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> None:
    year_files = sorted(BY_YEAR_DIR.glob("papers_*.jsonl"))
    if not year_files:
        print(f"ERROR: No year files found in {BY_YEAR_DIR}")
        return

    print(f"Found {len(year_files)} year files.")
    print("Scanning all papers (single pass)...")
    stats = collect_stats(BY_YEAR_DIR)

    sf_count    = len(stats["subfield_topics"])
    topic_count = len(stats["topic_to_subfield"])
    print(f"Found {sf_count} subfields, {topic_count} topics.")

    print("Building colour palette...")
    moc_colors, topic_colors, exemplar_colors = build_color_map(stats["subfield_topics"])

    VAULT_DIR.mkdir(parents=True, exist_ok=True)

    print("Writing overview MOC...")
    write_overview_moc(stats)

    print("Writing subfield MOCs...")
    write_subfield_mocs(stats)

    print("Writing topic notes...")
    write_topic_notes(stats)

    print("Writing exemplar paper notes...")
    n_exemplars = write_exemplar_notes(stats)
    print(f"  {n_exemplars} exemplar notes written.")

    print("Writing Obsidian config...")
    write_obsidian_config(moc_colors, topic_colors, exemplar_colors)

    total_notes = 1 + sf_count + topic_count + n_exemplars
    print()
    print("=" * 55)
    print(f"Vault ready: {VAULT_DIR}")
    print(f"  {total_notes} total notes  "
          f"(1 overview + {sf_count} subfields + {topic_count} topics + {n_exemplars} exemplars)")
    print()
    print("Open in Obsidian -> Open folder as vault -> Ctrl+G for graph")
    print("=" * 55)


if __name__ == "__main__":
    main()
