"""
DOSSIER — Entity Graph Analysis

Network analysis on top of entity_connections (co-occurrence edges) and
entity_resolutions (canonical mapping). Computes centrality, communities,
shortest paths, and neighborhood queries using networkx.

Usage:
    from dossier.core.graph_analysis import GraphAnalyzer

    with get_db() as conn:
        analyzer = GraphAnalyzer(conn)
        stats = analyzer.get_stats()
        top = analyzer.get_centrality(metric="betweenness", limit=10)
"""

import sqlite3
from dataclasses import dataclass, field
from typing import Optional

try:
    import networkx as nx

    HAS_NETWORKX = True
except ImportError:
    HAS_NETWORKX = False


def _require_networkx():
    if not HAS_NETWORKX:
        raise ImportError(
            "networkx is required for graph analysis. Install it with: pip install networkx>=3.0"
        )


# ═══════════════════════════════════════════════════════════════════
# Data Structures
# ═══════════════════════════════════════════════════════════════════


@dataclass
class NodeMetrics:
    """Centrality metrics for a single entity node."""

    entity_id: int
    name: str
    type: str
    degree: int = 0
    weighted_degree: int = 0
    betweenness: float = 0.0
    closeness: float = 0.0
    eigenvector: float = 0.0


@dataclass
class Community:
    """A detected community/cluster of entities."""

    id: int
    members: list[dict] = field(default_factory=list)
    size: int = 0
    density: float = 0.0


@dataclass
class PathResult:
    """Shortest path between two entities."""

    nodes: list[dict] = field(default_factory=list)
    edges: list[dict] = field(default_factory=list)
    total_weight: int = 0
    hops: int = 0


@dataclass
class GraphStats:
    """Summary statistics for the entity graph."""

    node_count: int = 0
    edge_count: int = 0
    density: float = 0.0
    components: int = 0
    avg_degree: float = 0.0
    avg_weighted_degree: float = 0.0


# ═══════════════════════════════════════════════════════════════════
# Engine
# ═══════════════════════════════════════════════════════════════════

VALID_METRICS = {"degree", "betweenness", "closeness", "eigenvector"}


class GraphAnalyzer:
    """Builds and analyzes the entity co-occurrence graph."""

    def __init__(self, conn: sqlite3.Connection):
        _require_networkx()
        self.conn = conn

    def _build_graph(self, entity_type: Optional[str] = None) -> "nx.Graph":
        """Load entity_connections, resolve canonical IDs, build nx.Graph.

        Edges pointing to resolved entities are merged into their canonical
        node. Duplicate edges have their weights summed.
        """
        G = nx.Graph()

        # Load resolution mapping: source_id → canonical_id
        resolutions = {}
        rows = self.conn.execute(
            "SELECT source_entity_id, canonical_entity_id FROM entity_resolutions"
        ).fetchall()
        for row in rows:
            resolutions[row["source_entity_id"]] = row["canonical_entity_id"]

        # Load all entities for node attributes
        entities = {}
        sql = "SELECT id, name, type FROM entities"
        params: list = []
        if entity_type:
            sql += " WHERE type = ?"
            params.append(entity_type)
        for row in self.conn.execute(sql, params).fetchall():
            entities[row["id"]] = {"name": row["name"], "type": row["type"]}

        # Collect type-filtered entity IDs for edge filtering
        type_ids = set(entities.keys()) if entity_type else None

        # Load edges, resolving canonical IDs
        edge_rows = self.conn.execute(
            "SELECT entity_a_id, entity_b_id, weight FROM entity_connections"
        ).fetchall()

        for row in edge_rows:
            a = resolutions.get(row["entity_a_id"], row["entity_a_id"])
            b = resolutions.get(row["entity_b_id"], row["entity_b_id"])

            # Skip self-loops from resolution merges
            if a == b:
                continue

            # Skip edges where either node is not in the type filter
            if type_ids is not None and (a not in type_ids or b not in type_ids):
                continue

            weight = row["weight"]

            # Add nodes with attributes (from canonical entity)
            for nid in (a, b):
                if nid not in G and nid in entities:
                    G.add_node(nid, **entities[nid])

            # Merge duplicate edges by summing weights
            if G.has_edge(a, b):
                G[a][b]["weight"] += weight
            else:
                if a in entities and b in entities:
                    G.add_edge(a, b, weight=weight)

        return G

    def get_stats(self, entity_type: Optional[str] = None) -> GraphStats:
        """Overall network statistics."""
        G = self._build_graph(entity_type)
        n = G.number_of_nodes()
        if n == 0:
            return GraphStats()

        degrees = [d for _, d in G.degree()]
        weighted_degrees = [d for _, d in G.degree(weight="weight")]

        return GraphStats(
            node_count=n,
            edge_count=G.number_of_edges(),
            density=nx.density(G),
            components=nx.number_connected_components(G),
            avg_degree=sum(degrees) / n,
            avg_weighted_degree=sum(weighted_degrees) / n,
        )

    def get_centrality(
        self,
        metric: str = "degree",
        entity_type: Optional[str] = None,
        limit: int = 50,
    ) -> list[NodeMetrics]:
        """Top entities by centrality metric.

        Supported metrics: degree, betweenness, closeness, eigenvector.
        """
        if metric not in VALID_METRICS:
            raise ValueError(
                f"Invalid metric '{metric}'. Must be one of: {', '.join(sorted(VALID_METRICS))}"
            )

        G = self._build_graph(entity_type)
        if G.number_of_nodes() == 0:
            return []

        # Compute requested centrality
        if metric == "degree":
            scores = nx.degree_centrality(G)
        elif metric == "betweenness":
            scores = nx.betweenness_centrality(G, weight="weight")
        elif metric == "closeness":
            scores = nx.closeness_centrality(G, distance="weight")
        else:  # eigenvector
            try:
                scores = nx.eigenvector_centrality(G, weight="weight", max_iter=1000)
            except nx.PowerIterationFailedConvergence:
                scores = {n: 0.0 for n in G.nodes()}

        # Also compute degree info for each node
        degrees = dict(G.degree())
        weighted_degrees = dict(G.degree(weight="weight"))

        # Build results sorted by the requested metric
        results = []
        for node_id, score in sorted(scores.items(), key=lambda x: x[1], reverse=True)[:limit]:
            attrs = G.nodes[node_id]
            nm = NodeMetrics(
                entity_id=node_id,
                name=attrs.get("name", ""),
                type=attrs.get("type", ""),
                degree=degrees.get(node_id, 0),
                weighted_degree=weighted_degrees.get(node_id, 0),
            )
            setattr(nm, metric, score)
            results.append(nm)

        return results

    def get_communities(
        self,
        entity_type: Optional[str] = None,
        min_size: int = 2,
    ) -> list[Community]:
        """Detect communities using Louvain method."""
        G = self._build_graph(entity_type)
        if G.number_of_nodes() == 0:
            return []

        communities = nx.community.louvain_communities(G, weight="weight", seed=42)

        results = []
        for idx, members in enumerate(communities):
            if len(members) < min_size:
                continue

            # Build subgraph for density calculation
            sub = G.subgraph(members)
            density = nx.density(sub)

            member_list = []
            for nid in sorted(members):
                attrs = G.nodes[nid]
                member_list.append(
                    {"entity_id": nid, "name": attrs.get("name", ""), "type": attrs.get("type", "")}
                )

            results.append(
                Community(id=idx, members=member_list, size=len(members), density=density)
            )

        # Sort by size descending
        results.sort(key=lambda c: c.size, reverse=True)
        return results

    def find_shortest_path(
        self,
        source_id: int,
        target_id: int,
    ) -> Optional[PathResult]:
        """Shortest path between two entities (Dijkstra, weight=1/co-occurrence).

        Returns None if no path exists or either entity is not in the graph.
        """
        G = self._build_graph()
        if source_id not in G or target_id not in G:
            return None

        # Invert weights: high co-occurrence = short distance
        G_inv = G.copy()
        for u, v, data in G_inv.edges(data=True):
            data["distance"] = 1.0 / data["weight"]

        try:
            path_nodes = nx.shortest_path(G_inv, source_id, target_id, weight="distance")
        except nx.NetworkXNoPath:
            return None

        nodes = []
        edges = []
        total_weight = 0

        for nid in path_nodes:
            attrs = G.nodes[nid]
            nodes.append(
                {"entity_id": nid, "name": attrs.get("name", ""), "type": attrs.get("type", "")}
            )

        for i in range(len(path_nodes) - 1):
            u, v = path_nodes[i], path_nodes[i + 1]
            w = G[u][v]["weight"]
            total_weight += w
            edges.append({"source": u, "target": v, "weight": w})

        return PathResult(
            nodes=nodes,
            edges=edges,
            total_weight=total_weight,
            hops=len(path_nodes) - 1,
        )

    def get_neighbors(
        self,
        entity_id: int,
        hops: int = 1,
        min_weight: int = 1,
    ) -> list[dict]:
        """BFS neighbors within N hops, filtered by min edge weight."""
        G = self._build_graph()
        if entity_id not in G:
            return []

        visited = {entity_id}
        frontier = {entity_id}
        results = []

        for _ in range(hops):
            next_frontier = set()
            for node in frontier:
                for neighbor in G.neighbors(node):
                    if neighbor in visited:
                        continue
                    weight = G[node][neighbor]["weight"]
                    if weight >= min_weight:
                        attrs = G.nodes[neighbor]
                        results.append(
                            {
                                "entity_id": neighbor,
                                "name": attrs.get("name", ""),
                                "type": attrs.get("type", ""),
                                "weight": weight,
                                "hop": _ + 1,
                            }
                        )
                        next_frontier.add(neighbor)
                    visited.add(neighbor)
            frontier = next_frontier

        # Sort by weight descending
        results.sort(key=lambda x: x["weight"], reverse=True)
        return results

    def get_subgraph(self, entity_ids: list[int]) -> dict:
        """Extract induced subgraph for the given entity IDs."""
        G = self._build_graph()
        valid_ids = [eid for eid in entity_ids if eid in G]

        if not valid_ids:
            return {"nodes": [], "edges": []}

        sub = G.subgraph(valid_ids)

        nodes = []
        for nid in sub.nodes():
            attrs = sub.nodes[nid]
            nodes.append(
                {"entity_id": nid, "name": attrs.get("name", ""), "type": attrs.get("type", "")}
            )

        edges = []
        for u, v, data in sub.edges(data=True):
            edges.append({"source": u, "target": v, "weight": data["weight"]})

        return {"nodes": nodes, "edges": edges}
