---
title: "Psychology Analytics"
type: "analytics-moc"
tags: [moc, analytics]
---

# Psychology Analytics

Analytical visualisations generated from the full 6.6M-paper psychology database.

← [[Psychology Overview]]

---

## 1. Topic Emergence Heatmap

How the top 50 topics have grown or shrunk across 1980–2024. Log-scaled — brighter = more papers.

![[1_topic_emergence_heatmap.png]]

---

## 2. Citation Inequality by Topic (Gini)

How unequally citations are distributed within each topic. A Gini of 1.0 means one paper takes all citations; 0 means perfectly even.

![[2_citation_gini.png]]

---

## 3. Volume vs. Citation Impact

Each bubble is a topic. X-axis = number of papers, Y-axis = median citations per paper, bubble size = total citation mass. Log–log axes.

![[3_volume_vs_impact.png]]

---

## 4. Rising vs. Declining Topics

Change in each topic's share of total psychology output between 2010–14 and 2020–24. Positive = growing, negative = shrinking.

![[4_rising_declining.png]]

---

## 5. Cross-Topic Co-occurrence Network

Topics linked when ≥500 papers cite both. Node size = paper count, edge weight = co-occurrence frequency. Spring layout.

![[5_cooccurrence_network.png]]

---

## 6. Citation Half-Life by Topic

The citation-weighted median publication year for each topic. Topics on the right cite newer work; topics on the left cite older literature.

![[6_citation_halflife.png]]

---

## 7. Citation Power Law

Does psychology follow a power law? Log-log CCDF of citation counts with a fitted line. Exponent α ≈ 1.57 — a heavy tail, but shallower than physics or economics.

![[7_power_law.png]]

---

## 8. Abstract Length vs. Citations

Does writing more correlate with being cited more? Hexbin density plot per subfield, with median citation line overlaid.

![[8_abstract_length_vs_citations.png]]
