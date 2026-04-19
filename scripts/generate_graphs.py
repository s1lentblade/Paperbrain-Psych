#!/usr/bin/env python3
"""
generate_graphs.py — 8 analytical graphs from the PaperBrain psychology DB.
Output: data/graphs/*.png

pip install matplotlib seaborn numpy networkx
"""

import sqlite3
import time
import warnings
from collections import defaultdict
from pathlib import Path

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import matplotlib.ticker as mticker

warnings.filterwarnings("ignore")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

SCRIPT_DIR = Path(__file__).parent
DB_PATH    = SCRIPT_DIR.parent / "data" / "papers.db"
OUT_DIR    = SCRIPT_DIR.parent / "data" / "graphs"
OUT_DIR.mkdir(exist_ok=True)

SUBFIELD_PALETTE = {
    "Experimental and Cognitive Psychology":        "#4E79A7",
    "Applied Psychology":                           "#F28E2B",
    "Clinical Psychology":                          "#E15759",
    "Social Psychology":                            "#76B7B2",
    "Developmental and Educational Psychology":     "#59A14F",
    "Neuropsychology and Physiological Psychology": "#EDC948",
    "General Psychology":                           "#B07AA1",
}
SF_ORDER = list(SUBFIELD_PALETTE)
FALLBACK  = "#888899"

DARK_BG   = "#0f0f1a"
PANEL_BG  = "#161625"
GRID_COL  = "#2a2a42"
TEXT_COL  = "#ddddf0"
MUTED_COL = "#7777aa"

plt.rcParams.update({
    "figure.facecolor":  DARK_BG,
    "axes.facecolor":    PANEL_BG,
    "axes.edgecolor":    GRID_COL,
    "axes.labelcolor":   TEXT_COL,
    "xtick.color":       MUTED_COL,
    "ytick.color":       MUTED_COL,
    "text.color":        TEXT_COL,
    "grid.color":        GRID_COL,
    "grid.alpha":        0.5,
    "font.family":       "sans-serif",
    "font.size":         9,
    "axes.titlesize":    11,
    "axes.titleweight":  "bold",
    "axes.titlecolor":   TEXT_COL,
})


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(str(DB_PATH))
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA cache_size=-262144")
    conn.execute("PRAGMA query_only=ON")
    return conn


def trunc(s: str, n: int = 36) -> str:
    return s[: n - 1] + "…" if len(s) > n else s


def sf_col(sf: str) -> str:
    return SUBFIELD_PALETTE.get(sf, FALLBACK)


def gini(values) -> float:
    a = np.sort(np.asarray(values, dtype=float))
    n = len(a)
    if n == 0 or a.sum() == 0:
        return 0.0
    idx = np.arange(1, n + 1)
    return float((2 * (idx * a).sum() / (n * a.sum())) - (n + 1) / n)


def save(fig, name: str) -> None:
    path = OUT_DIR / name
    fig.savefig(path, dpi=150, bbox_inches="tight", facecolor=DARK_BG)
    plt.close(fig)
    print(f"  saved -> {path.name}")


def legend_patches(palette=SUBFIELD_PALETTE):
    return [
        mpatches.Patch(color=c, label=trunc(sf, 42))
        for sf, c in palette.items()
    ]


def step(n: int, title: str) -> None:
    print(f"\n[{n}/8] {title} …", flush=True)


# ---------------------------------------------------------------------------
# 1. Topic Emergence Heatmap
# ---------------------------------------------------------------------------

def chart_1_heatmap(conn):
    step(1, "Topic emergence heatmap")
    t0 = time.time()

    rows = conn.execute("""
        SELECT primary_topic, year, COUNT(*) AS n
        FROM papers
        WHERE year BETWEEN 1980 AND 2024
          AND primary_topic != ''
        GROUP BY primary_topic, year
    """).fetchall()

    # Build pivot: topic -> year -> count
    topic_year: dict[str, dict[int, int]] = defaultdict(lambda: defaultdict(int))
    topic_total: dict[str, int] = defaultdict(int)
    for r in rows:
        topic_year[r["primary_topic"]][r["year"]] += r["n"]
        topic_total[r["primary_topic"]] += r["n"]

    # Keep top 50 topics by total paper count for readability
    top50 = sorted(topic_total, key=topic_total.get, reverse=True)[:50]
    years  = list(range(1980, 2025))

    matrix = np.zeros((len(top50), len(years)), dtype=float)
    for i, tp in enumerate(top50):
        for j, yr in enumerate(years):
            matrix[i, j] = topic_year[tp].get(yr, 0)

    # Log-scale for color (handles 3-order magnitude range)
    log_matrix = np.log1p(matrix)

    fig, ax = plt.subplots(figsize=(18, 14))
    im = ax.imshow(log_matrix, aspect="auto", cmap="magma",
                   interpolation="nearest", origin="upper")

    ax.set_xticks(range(0, len(years), 5))
    ax.set_xticklabels([years[i] for i in range(0, len(years), 5)], fontsize=8)
    ax.set_yticks(range(len(top50)))
    ax.set_yticklabels([trunc(t, 42) for t in top50], fontsize=7.5)
    ax.set_xlabel("Year", labelpad=8)
    ax.set_title("Topic Emergence Heatmap  (log paper count, top 50 topics)", pad=12)

    cbar = fig.colorbar(im, ax=ax, fraction=0.02, pad=0.01)
    cbar.set_label("log(1 + papers)", color=TEXT_COL)
    cbar.ax.yaxis.set_tick_params(color=MUTED_COL)
    plt.setp(cbar.ax.yaxis.get_ticklabels(), color=MUTED_COL)

    fig.tight_layout()
    save(fig, "1_topic_emergence_heatmap.png")
    print(f"    {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# 2. Citation Gini Coefficient by Topic
# ---------------------------------------------------------------------------

def chart_2_gini(conn):
    step(2, "Citation Gini by topic")
    t0 = time.time()

    # Fetch all (topic, citation) pairs — ~5M rows but only 2 cols
    rows = conn.execute("""
        SELECT primary_topic, cited_by_count, subfield
        FROM papers
        WHERE cited_by_count > 0 AND primary_topic != ''
        ORDER BY primary_topic
    """).fetchall()

    topic_data: dict[str, dict] = defaultdict(lambda: {"cits": [], "sf": ""})
    for r in rows:
        topic_data[r["primary_topic"]]["cits"].append(r["cited_by_count"])
        topic_data[r["primary_topic"]]["sf"] = r["subfield"]

    results = sorted(
        [
            (tp, gini(d["cits"]), d["sf"], len(d["cits"]))
            for tp, d in topic_data.items()
        ],
        key=lambda x: x[1],
    )

    labels  = [trunc(r[0], 42) for r in results]
    ginis   = [r[1] for r in results]
    colors  = [sf_col(r[2]) for r in results]

    fig, ax = plt.subplots(figsize=(13, 18))
    bars = ax.barh(range(len(results)), ginis, color=colors, edgecolor="none",
                   height=0.75, alpha=0.85)
    ax.set_yticks(range(len(results)))
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.set_xlabel("Gini coefficient  (0 = equal, 1 = one paper holds all citations)")
    ax.set_title("Citation Inequality by Topic", pad=12)
    ax.set_xlim(0, 1)
    ax.xaxis.grid(True)
    ax.set_axisbelow(True)

    # Vertical line at median
    med = float(np.median(ginis))
    ax.axvline(med, color="#ffffff", lw=0.8, ls="--", alpha=0.5,
               label=f"Median = {med:.3f}")
    ax.legend(loc="lower right", framealpha=0.2)

    fig.legend(handles=legend_patches(), loc="lower center",
               ncol=2, fontsize=7.5, framealpha=0.15,
               bbox_to_anchor=(0.5, -0.01))
    fig.tight_layout()
    save(fig, "2_citation_gini.png")
    print(f"    {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# 3. Volume vs. Citation Impact Scatter (bubble)
# ---------------------------------------------------------------------------

def chart_3_scatter(conn):
    step(3, "Volume vs. impact scatter")
    t0 = time.time()

    rows2 = conn.execute("""
        SELECT primary_topic, subfield,
               COUNT(*)          AS n,
               AVG(cited_by_count) AS avg_cit,
               SUM(cited_by_count) AS total_cit
        FROM papers
        WHERE primary_topic != ''
        GROUP BY primary_topic
    """).fetchall()

    # Get medians per topic
    med_rows = conn.execute("""
        SELECT primary_topic, cited_by_count
        FROM papers WHERE primary_topic != '' AND cited_by_count >= 0
        ORDER BY primary_topic, cited_by_count
    """).fetchall()
    topic_cits: dict[str, list] = defaultdict(list)
    for r in med_rows:
        topic_cits[r["primary_topic"]].append(r["cited_by_count"])
    medians = {tp: float(np.median(v)) for tp, v in topic_cits.items()}

    topics = [(r["primary_topic"], r["subfield"], r["n"],
               medians.get(r["primary_topic"], 0), r["total_cit"])
              for r in rows2]

    x      = np.array([t[2]  for t in topics], dtype=float)   # paper count
    y      = np.array([t[3]  for t in topics], dtype=float)   # median citations
    sizes  = np.array([t[4]  for t in topics], dtype=float)   # total citations
    colors = [sf_col(t[1]) for t in topics]
    labels = [t[0]  for t in topics]

    # Normalise bubble sizes
    size_scaled = 30 + 1200 * (sizes - sizes.min()) / (sizes.max() - sizes.min() + 1)

    fig, ax = plt.subplots(figsize=(14, 9))
    sc = ax.scatter(x, y, s=size_scaled, c=colors, alpha=0.75, edgecolors="#333355",
                    linewidths=0.5)

    ax.set_xscale("log")
    ax.set_yscale("log")
    ax.set_xlabel("Number of papers in topic  (log scale)", labelpad=8)
    ax.set_ylabel("Median citations per paper  (log scale)", labelpad=8)
    ax.set_title("Topic Volume vs. Citation Impact\n"
                 "(bubble size = total citation mass)", pad=12)
    ax.grid(True, which="both")
    ax.set_axisbelow(True)

    # Label the most interesting outliers (top 10 by median citations and top 10 by n)
    annotate = set()
    for idx in np.argsort(y)[-8:]:
        annotate.add(idx)
    for idx in np.argsort(x)[-5:]:
        annotate.add(idx)
    for idx in annotate:
        ax.annotate(trunc(labels[idx], 30), (x[idx], y[idx]),
                    fontsize=6.5, color=TEXT_COL, alpha=0.85,
                    xytext=(4, 4), textcoords="offset points")

    fig.legend(handles=legend_patches(), loc="lower left",
               ncol=1, fontsize=7.5, framealpha=0.15,
               bbox_to_anchor=(0.01, 0.01))
    fig.tight_layout()
    save(fig, "3_volume_vs_impact.png")
    print(f"    {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# 4. Rising vs. Declining Topics
# ---------------------------------------------------------------------------

def chart_4_rising(conn):
    step(4, "Rising vs declining topics")
    t0 = time.time()

    rows = conn.execute("""
        SELECT primary_topic, subfield,
               SUM(CASE WHEN year BETWEEN 2010 AND 2014 THEN 1 ELSE 0 END) AS early,
               SUM(CASE WHEN year BETWEEN 2020 AND 2024 THEN 1 ELSE 0 END) AS late,
               COUNT(*) AS total
        FROM papers
        WHERE primary_topic != ''
        GROUP BY primary_topic
        HAVING early > 0 AND late > 0
    """).fetchall()

    # Compute share change per million (normalise by total papers in each window)
    total_early = conn.execute(
        "SELECT COUNT(*) FROM papers WHERE year BETWEEN 2010 AND 2014"
    ).fetchone()[0]
    total_late  = conn.execute(
        "SELECT COUNT(*) FROM papers WHERE year BETWEEN 2020 AND 2024"
    ).fetchone()[0]

    data = []
    for r in rows:
        share_early = r["early"] / total_early * 1000
        share_late  = r["late"]  / total_late  * 1000
        delta = share_late - share_early
        data.append((r["primary_topic"], r["subfield"], delta, r["total"]))

    data.sort(key=lambda x: x[2])

    labels  = [trunc(d[0], 44) for d in data]
    deltas  = [d[2] for d in data]
    colors  = [sf_col(d[1]) for d in data]

    fig, ax = plt.subplots(figsize=(13, 18))
    ax.barh(range(len(data)), deltas, color=colors, edgecolor="none",
            height=0.75, alpha=0.85)
    ax.axvline(0, color=TEXT_COL, lw=0.8, alpha=0.6)
    ax.set_yticks(range(len(data)))
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.set_xlabel("Change in papers per 1,000  (2020–24 share minus 2010–14 share)")
    ax.set_title("Rising vs. Declining Topics\n"
                 "(share of total psychology output, 2010–14 -> 2020–24)", pad=12)
    ax.xaxis.grid(True)
    ax.set_axisbelow(True)

    fig.legend(handles=legend_patches(), loc="lower center",
               ncol=2, fontsize=7.5, framealpha=0.15,
               bbox_to_anchor=(0.5, -0.01))
    fig.tight_layout()
    save(fig, "4_rising_declining.png")
    print(f"    {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# 5. Cross-topic Co-occurrence Network
# ---------------------------------------------------------------------------

def chart_5_network(conn):
    step(5, "Cross-topic co-occurrence network")
    t0 = time.time()

    try:
        import networkx as nx
    except ImportError:
        print("    SKIP — pip install networkx")
        return

    # Fetch paper_topics filtered to the 144 psychology primary topics
    psych_topics = {r["primary_topic"] for r in conn.execute(
        "SELECT DISTINCT primary_topic FROM papers WHERE primary_topic != ''"
    ).fetchall()}

    rows = conn.execute("""
        SELECT paper_id, topic_name
        FROM paper_topics
        WHERE topic_name IN (SELECT DISTINCT primary_topic FROM papers
                             WHERE primary_topic != '')
        ORDER BY paper_id
    """).fetchall()

    paper_topics: dict[str, list[str]] = defaultdict(list)
    for r in rows:
        paper_topics[r["paper_id"]].append(r["topic_name"])

    # Count co-occurrences
    cooccur: dict[tuple, int] = defaultdict(int)
    for topics in paper_topics.values():
        uniq = list(set(topics))
        for i in range(len(uniq)):
            for j in range(i + 1, len(uniq)):
                pair = tuple(sorted((uniq[i], uniq[j])))
                cooccur[pair] += 1

    # Only keep edges with >= 500 co-occurring papers
    MIN_EDGE = 500
    G = nx.Graph()
    for (a, b), w in cooccur.items():
        if w >= MIN_EDGE:
            G.add_edge(a, b, weight=w)

    # Node metadata from papers table
    topic_meta = {r["primary_topic"]: (r["subfield"], r["n"])
                  for r in conn.execute("""
                      SELECT primary_topic, subfield, COUNT(*) AS n
                      FROM papers WHERE primary_topic != ''
                      GROUP BY primary_topic
                  """).fetchall()}

    print(f"    {G.number_of_nodes()} nodes, {G.number_of_edges()} edges")

    pos = nx.spring_layout(G, weight="weight", k=2.5, seed=42, iterations=80)

    node_sizes  = []
    node_colors = []
    for node in G.nodes():
        meta = topic_meta.get(node, ("", 1000))
        node_sizes.append(max(30, meta[1] / 300))
        node_colors.append(sf_col(meta[0]))

    edge_weights = [G[u][v]["weight"] for u, v in G.edges()]
    max_w = max(edge_weights) if edge_weights else 1
    edge_widths = [0.3 + 2.5 * (w / max_w) for w in edge_weights]
    edge_alpha  = [0.15 + 0.6 * (w / max_w) for w in edge_weights]

    fig, ax = plt.subplots(figsize=(16, 14))

    for (u, v), lw, alpha in zip(G.edges(), edge_widths, edge_alpha):
        x_vals = [pos[u][0], pos[v][0]]
        y_vals = [pos[u][1], pos[v][1]]
        ax.plot(x_vals, y_vals, color="#6666aa", lw=lw, alpha=alpha, zorder=1)

    ax.scatter(
        [pos[n][0] for n in G.nodes()],
        [pos[n][1] for n in G.nodes()],
        s=node_sizes, c=node_colors, zorder=2,
        edgecolors="#222233", linewidths=0.4, alpha=0.9,
    )

    # Label the 25 most-connected nodes
    degree = dict(G.degree())
    top_nodes = sorted(degree, key=degree.get, reverse=True)[:25]
    for node in top_nodes:
        ax.annotate(trunc(node, 28), pos[node],
                    fontsize=6, color=TEXT_COL, alpha=0.85,
                    ha="center", va="bottom", xytext=(0, 4),
                    textcoords="offset points")

    ax.set_title(f"Cross-topic Co-occurrence Network\n"
                 f"(edges = ≥{MIN_EDGE:,} papers spanning both topics)", pad=12)
    ax.axis("off")

    fig.legend(handles=legend_patches(), loc="lower center",
               ncol=2, fontsize=7.5, framealpha=0.15,
               bbox_to_anchor=(0.5, -0.01))
    fig.tight_layout()
    save(fig, "5_cooccurrence_network.png")
    print(f"    {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# 6. Citation Half-life by Topic
# ---------------------------------------------------------------------------

def chart_6_halflife(conn):
    step(6, "Citation half-life")
    t0 = time.time()

    rows = conn.execute("""
        SELECT primary_topic, subfield, year, cited_by_count
        FROM papers
        WHERE year IS NOT NULL AND cited_by_count > 0 AND primary_topic != ''
    """).fetchall()

    topic_data: dict[str, dict] = defaultdict(lambda: {"years": [], "cits": [], "sf": ""})
    for r in rows:
        d = topic_data[r["primary_topic"]]
        d["years"].append(r["year"])
        d["cits"].append(r["cited_by_count"])
        d["sf"] = r["subfield"]

    # Citation-weighted median year
    results = []
    for tp, d in topic_data.items():
        yrs  = np.array(d["years"], dtype=float)
        cits = np.array(d["cits"],  dtype=float)
        # weighted median
        order = np.argsort(yrs)
        yrs_s, cits_s = yrs[order], cits[order]
        cumw  = np.cumsum(cits_s)
        half  = cumw[-1] / 2
        w_med = float(yrs_s[np.searchsorted(cumw, half)])
        results.append((tp, d["sf"], w_med))

    results.sort(key=lambda x: x[2])

    labels  = [trunc(r[0], 44) for r in results]
    medians = [r[2] for r in results]
    colors  = [sf_col(r[1]) for r in results]

    overall_med = float(np.median(medians))

    fig, ax = plt.subplots(figsize=(13, 18))
    ax.barh(range(len(results)), medians, color=colors,
            edgecolor="none", height=0.75, alpha=0.85)
    ax.axvline(overall_med, color="#ffffff", lw=0.9, ls="--", alpha=0.6,
               label=f"Median = {overall_med:.0f}")
    ax.set_yticks(range(len(results)))
    ax.set_yticklabels(labels, fontsize=7.5)
    ax.set_xlabel("Citation-weighted median publication year of cited papers")
    ax.set_title("Citation Half-life by Topic\n"
                 "(older = field lives on classic works; newer = fast-moving)", pad=12)
    ax.xaxis.grid(True)
    ax.set_axisbelow(True)
    ax.legend(loc="lower right", framealpha=0.2)

    # Shade eras
    ax.axvspan(1900, 1990, color="#ffffff", alpha=0.03, label="Pre-1990")
    ax.axvspan(2010, 2030, color="#4E79A7", alpha=0.05, label="Post-2010")

    fig.legend(handles=legend_patches(), loc="lower center",
               ncol=2, fontsize=7.5, framealpha=0.15,
               bbox_to_anchor=(0.5, -0.01))
    fig.tight_layout()
    save(fig, "6_citation_halflife.png")
    print(f"    {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# 7. Power Law / Citation Distribution
# ---------------------------------------------------------------------------

def chart_7_powerlaw(conn):
    step(7, "Citation power law")
    t0 = time.time()

    # Aggregate counts server-side to avoid 6M row fetch
    rows = conn.execute("""
        SELECT cited_by_count AS c, COUNT(*) AS n
        FROM papers
        WHERE cited_by_count > 0
        GROUP BY cited_by_count
        ORDER BY cited_by_count
    """).fetchall()

    cits = np.array([r["c"] for r in rows], dtype=float)
    cnts = np.array([r["n"] for r in rows], dtype=float)

    # Total papers per citation bin (for CCDF)
    total = cnts.sum()
    ccdf  = 1 - np.cumsum(cnts) / total     # P(X > c)

    # Fit power law on log-log (linear regression)
    mask  = (cits >= 10) & (cits <= 10000)
    log_c = np.log10(cits[mask])
    log_f = np.log10(ccdf[mask] + 1e-12)
    slope, intercept = np.polyfit(log_c, log_f, 1)

    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    # Left: raw histogram (log-log)
    ax = axes[0]
    ax.loglog(cits, cnts, ".", color="#76B7B2", alpha=0.4, ms=3, label="Observed")
    ax.set_xlabel("Citations per paper")
    ax.set_ylabel("Number of papers")
    ax.set_title("Citation Frequency Distribution")
    ax.grid(True, which="both")
    ax.set_axisbelow(True)

    # Right: CCDF (complementary CDF) + fitted line
    ax = axes[1]
    ax.loglog(cits, ccdf + 1e-9, color="#4E79A7", lw=0.5, alpha=0.6, label="Empirical CCDF")

    fit_x = np.logspace(1, 4, 100)
    fit_y = 10 ** (intercept + slope * np.log10(fit_x))
    ax.loglog(fit_x, fit_y, "--", color="#E15759", lw=1.5,
              label=f"Power law fit  alpha = {-slope:.2f}")

    ax.set_xlabel("Citations (log scale)")
    ax.set_ylabel("P(citations > x)")
    ax.set_title("CCDF with Power Law Fit")
    ax.legend(framealpha=0.2)
    ax.grid(True, which="both")
    ax.set_axisbelow(True)

    fig.suptitle("Citation Distribution — Does Psychology Follow a Power Law?",
                 fontsize=12, fontweight="bold", color=TEXT_COL, y=1.02)
    fig.tight_layout()
    save(fig, "7_power_law.png")
    print(f"    exponent alpha = {-slope:.3f}  ({time.time()-t0:.1f}s)")


# ---------------------------------------------------------------------------
# 8. Abstract Length vs. Citations (by subfield)
# ---------------------------------------------------------------------------

def chart_8_abstract(conn):
    step(8, "Abstract length vs. citations")
    t0 = time.time()

    rows = conn.execute("""
        SELECT subfield,
               LENGTH(abstract)  AS abs_len,
               cited_by_count    AS cit
        FROM papers
        WHERE abstract IS NOT NULL AND abstract != ''
          AND cited_by_count > 0
          AND subfield != ''
          AND LENGTH(abstract) BETWEEN 100 AND 3000
        ORDER BY RANDOM()
        LIMIT 120000
    """).fetchall()

    sf_data: dict[str, dict] = defaultdict(lambda: {"lens": [], "cits": []})
    for r in rows:
        sf_data[r["subfield"]]["lens"].append(r["abs_len"])
        sf_data[r["subfield"]]["cits"].append(r["cit"])

    fig, axes = plt.subplots(2, 4, figsize=(18, 9), sharey=False)
    axes_flat = axes.flatten()

    subfields = [sf for sf in SF_ORDER if sf in sf_data]
    for ax, sf in zip(axes_flat, subfields):
        lens = np.array(sf_data[sf]["lens"], dtype=float)
        cits = np.log1p(np.array(sf_data[sf]["cits"], dtype=float))

        # Bin abstract lengths and show median citations per bin
        bins = np.percentile(lens, np.linspace(0, 100, 21))
        bins = np.unique(bins)
        bin_ids    = np.digitize(lens, bins)
        bin_medians = []
        bin_centers = []
        bin_counts  = []
        for b in range(1, len(bins)):
            mask = bin_ids == b
            if mask.sum() < 10:
                continue
            bin_medians.append(np.median(cits[mask]))
            bin_centers.append((bins[b - 1] + bins[b]) / 2)
            bin_counts.append(mask.sum())

        color = sf_col(sf)
        # Hex scatter background
        ax.hexbin(lens, cits, gridsize=30, cmap="Greys", bins="log",
                  mincnt=1, alpha=0.5)
        ax.plot(bin_centers, bin_medians, "o-", color=color,
                ms=5, lw=1.5, label="Median log(cit)")

        ax.set_title(trunc(sf, 34), color=color, fontsize=8, fontweight="bold")
        ax.set_xlabel("Abstract length (chars)", fontsize=7.5)
        ax.set_ylabel("log(1 + citations)", fontsize=7.5)
        ax.grid(True, alpha=0.3)

    for ax in axes_flat[len(subfields):]:
        ax.axis("off")

    fig.suptitle("Abstract Length vs. Citation Impact, by Subfield\n"
                 "(colored line = median log-citations per length bin)",
                 fontsize=11, fontweight="bold", color=TEXT_COL)
    fig.tight_layout()
    save(fig, "8_abstract_length_vs_citations.png")
    print(f"    {time.time()-t0:.1f}s")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    if not DB_PATH.exists():
        raise FileNotFoundError(f"DB not found: {DB_PATH}")

    print(f"PaperBrain graphs -> {OUT_DIR}")
    t_start = time.time()

    conn = connect()

    chart_1_heatmap(conn)
    chart_2_gini(conn)
    chart_3_scatter(conn)
    chart_4_rising(conn)
    chart_5_network(conn)
    chart_6_halflife(conn)
    chart_7_powerlaw(conn)
    chart_8_abstract(conn)

    conn.close()
    print(f"\nAll 8 graphs done in {time.time()-t_start:.0f}s  ->  {OUT_DIR}")


if __name__ == "__main__":
    main()
