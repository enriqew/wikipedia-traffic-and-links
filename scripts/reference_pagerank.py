"""PageRank on one machine with NetworkX, as the oracle for wikihadoop.PageRank (plan section 14).

Reads the adjacency run-graph.sh copied out ($WH_DATA_DIR/graphs/<wiki>-<date>/graph.adjacency.tsv),
every article a node whether or not it has links, and runs networkx.pagerank with alpha 0.85, a
uniform teleport and dangling mass shared uniformly (NetworkX's defaults), converged far past the
MapReduce run: tol 10^-15 per node. Writes $WH_DATA_DIR/pagerank/<wiki>-<date>/reference.tsv,
page_id and rank (summing to 1), which scripts/check_pagerank.py compares with the cluster's ranks.

Simple English only: NetworkX holds the whole graph in memory.

Usage: python scripts/reference_pagerank.py simplewiki 20261101
"""

import os
import sys
import time
from pathlib import Path

import networkx as nx


def main() -> None:
    wiki, date = sys.argv[1], sys.argv[2]
    root = Path(os.environ.get("WH_DATA_DIR", "data"))
    started = time.time()
    g = nx.DiGraph()
    with open(root / "graphs" / f"{wiki}-{date}" / "graph.adjacency.tsv", encoding="utf-8") as f:
        for line in f:
            page, degree, targets = line.rstrip("\n").split("\t")
            page = int(page)
            g.add_node(page)
            if targets:
                links = [int(t) for t in targets.split(",")]
                if len(links) != int(degree):
                    raise SystemExit(f"{page}: out-degree {degree}, {len(links)} links")
                g.add_edges_from((page, t) for t in links)
    print(f"{g.number_of_nodes():,} articles, {g.number_of_edges():,} edges loaded in {time.time() - started:.0f} s")
    ranks = nx.pagerank(g, alpha=0.85, tol=1e-15, max_iter=1000)
    out = root / "pagerank" / f"{wiki}-{date}"
    out.mkdir(parents=True, exist_ok=True)
    with open(out / "reference.tsv", "w", encoding="utf-8", newline="\n") as f:
        for page in sorted(ranks):
            f.write(f"{page}\t{ranks[page]!r}\n")
    print(f"pagerank in {time.time() - started:.0f} s, sum {sum(ranks.values()):.15f}")


if __name__ == "__main__":
    main()
