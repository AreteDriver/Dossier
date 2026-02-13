"""Tests for dossier.core.graph_analysis — Entity Graph Analysis engine."""

import importlib
import sqlite3
from unittest.mock import patch

import pytest

from dossier.core.graph_analysis import (
    Community,
    GraphAnalyzer,
    NodeMetrics,
    PathResult,
)


# ═══════════════════════════════════════════════════════════════════
# Fixtures
# ═══════════════════════════════════════════════════════════════════

# Topology (star + triangle):
#
#     A(person) --5-- B(person) --3-- C(person)
#          |                          |
#          2                          1
#          |                          |
#     D(org) --------4------- E(place)
#
# Plus F(person) isolated (disconnected)
# Plus G(person) resolved to A (canonical merge test)


@pytest.fixture
def graph_db(tmp_path):
    """In-memory SQLite with seeded entities + connections for graph tests."""
    import dossier.db.database as db_mod

    db_path = str(tmp_path / "graph_test.db")
    # Temporarily set DB_PATH so init_db works
    original = db_mod.DB_PATH
    db_mod.DB_PATH = db_path
    db_mod.init_db()
    db_mod.DB_PATH = original

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")

    # Insert entities
    entities = [
        (1, "Alice", "person", "alice"),
        (2, "Bob", "person", "bob"),
        (3, "Carol", "person", "carol"),
        (4, "Acme Corp", "org", "acme corp"),
        (5, "New York", "place", "new york"),
        (6, "Frank", "person", "frank"),  # isolated
        (7, "Alice Alias", "person", "alice alias"),  # will resolve to Alice
    ]
    conn.executemany(
        "INSERT INTO entities (id, name, type, canonical) VALUES (?, ?, ?, ?)", entities
    )

    # Insert connections (star + triangle)
    connections = [
        (1, 2, 5),  # Alice-Bob weight 5
        (2, 3, 3),  # Bob-Carol weight 3
        (1, 4, 2),  # Alice-Acme weight 2
        (3, 5, 1),  # Carol-NYC weight 1
        (4, 5, 4),  # Acme-NYC weight 4
    ]
    conn.executemany(
        "INSERT INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
        connections,
    )

    # Insert resolution: G (id=7) → A (id=1)
    conn.execute(
        "INSERT INTO entity_resolutions (source_entity_id, canonical_entity_id) VALUES (?, ?)",
        (7, 1),
    )

    # Add an edge from G to Bob (weight 2) — should merge onto Alice-Bob
    conn.execute(
        "INSERT INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
        (7, 2, 2),
    )

    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def analyzer(graph_db):
    """GraphAnalyzer instance backed by the seeded graph_db."""
    return GraphAnalyzer(graph_db)


@pytest.fixture
def empty_db(tmp_path):
    """Empty database with schema but no data."""
    import dossier.db.database as db_mod

    db_path = str(tmp_path / "empty_graph.db")
    original = db_mod.DB_PATH
    db_mod.DB_PATH = db_path
    db_mod.init_db()
    db_mod.DB_PATH = original

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys=ON")
    yield conn
    conn.close()


# ═══════════════════════════════════════════════════════════════════
# Build Graph
# ═══════════════════════════════════════════════════════════════════


class TestBuildGraph:
    def test_builds_graph_with_nodes_and_edges(self, analyzer):
        G = analyzer._build_graph()
        # 5 connected nodes (A,B,C,D,E) — F is isolated (no edges), G merged into A
        assert G.number_of_nodes() == 5
        assert G.number_of_edges() == 5

    def test_resolution_merges_edges(self, analyzer):
        """Edge G-B (weight 2) should merge onto A-B, giving total weight 7."""
        G = analyzer._build_graph()
        assert G[1][2]["weight"] == 7  # 5 (original) + 2 (from alias)

    def test_self_loops_skipped(self, analyzer):
        """If G resolves to A, an edge G-A would be a self-loop and skipped."""
        # Add a G-A edge
        analyzer.conn.execute(
            "INSERT INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
            (7, 1, 10),
        )
        analyzer.conn.commit()
        G = analyzer._build_graph()
        assert not G.has_edge(1, 1)

    def test_type_filter(self, analyzer):
        """Filtering by type='person' excludes org/place nodes."""
        G = analyzer._build_graph(entity_type="person")
        for nid in G.nodes():
            assert G.nodes[nid]["type"] == "person"
        # Person-to-person edges: Alice-Bob(7), Bob-Carol(3)
        assert G.number_of_nodes() == 3
        assert G.has_edge(1, 2)
        assert G.has_edge(2, 3)

    def test_empty_graph(self, empty_db):
        a = GraphAnalyzer(empty_db)
        G = a._build_graph()
        assert G.number_of_nodes() == 0
        assert G.number_of_edges() == 0


# ═══════════════════════════════════════════════════════════════════
# Stats
# ═══════════════════════════════════════════════════════════════════


class TestGraphStats:
    def test_stats_values(self, analyzer):
        stats = analyzer.get_stats()
        assert stats.node_count == 5
        assert stats.edge_count == 5
        assert stats.density > 0
        assert stats.components == 1  # all connected
        assert stats.avg_degree > 0
        assert stats.avg_weighted_degree > 0

    def test_stats_empty(self, empty_db):
        a = GraphAnalyzer(empty_db)
        stats = a.get_stats()
        assert stats.node_count == 0
        assert stats.edge_count == 0
        assert stats.density == 0.0

    def test_stats_with_type_filter(self, analyzer):
        stats = analyzer.get_stats(entity_type="person")
        # Alice, Bob, Carol connected among persons
        assert stats.node_count == 3
        assert stats.edge_count == 2

    def test_stats_components_with_isolate(self, graph_db):
        """Add an isolated person-person edge to get 2 components when filtering."""
        # Add connection between Frank (6) and a new person
        graph_db.execute(
            "INSERT INTO entities (id, name, type, canonical) VALUES (?, ?, ?, ?)",
            (8, "Eve", "person", "eve"),
        )
        graph_db.execute(
            "INSERT INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
            (6, 8, 1),
        )
        graph_db.commit()
        a = GraphAnalyzer(graph_db)
        stats = a.get_stats(entity_type="person")
        assert stats.components == 2  # Alice-Bob and Frank-Eve


# ═══════════════════════════════════════════════════════════════════
# Centrality
# ═══════════════════════════════════════════════════════════════════


class TestCentrality:
    def test_degree_centrality(self, analyzer):
        results = analyzer.get_centrality(metric="degree")
        assert len(results) > 0
        assert isinstance(results[0], NodeMetrics)
        # All nodes have degree info
        for r in results:
            assert r.degree > 0

    def test_degree_top_node(self, analyzer):
        """Alice has most connections: Bob(7), Acme(2) — degree 2 out of 4 neighbors max."""
        results = analyzer.get_centrality(metric="degree", limit=1)
        # Alice (id=1) should be among the top
        assert results[0].entity_id == 1

    def test_betweenness(self, analyzer):
        results = analyzer.get_centrality(metric="betweenness")
        assert len(results) > 0
        # At least some node should have nonzero betweenness
        assert any(r.betweenness > 0 for r in results)

    def test_closeness(self, analyzer):
        results = analyzer.get_centrality(metric="closeness")
        assert len(results) > 0
        assert all(r.closeness >= 0 for r in results)

    def test_eigenvector(self, analyzer):
        results = analyzer.get_centrality(metric="eigenvector")
        assert len(results) > 0

    def test_limit_param(self, analyzer):
        results = analyzer.get_centrality(metric="degree", limit=2)
        assert len(results) == 2

    def test_invalid_metric(self, analyzer):
        with pytest.raises(ValueError, match="Invalid metric"):
            analyzer.get_centrality(metric="invalid")

    def test_centrality_empty(self, empty_db):
        a = GraphAnalyzer(empty_db)
        results = a.get_centrality()
        assert results == []

    def test_centrality_type_filter(self, analyzer):
        results = analyzer.get_centrality(metric="degree", entity_type="person")
        for r in results:
            assert r.type == "person"

    def test_eigenvector_convergence_failure(self, analyzer):
        """Eigenvector centrality fallback when power iteration fails."""
        import networkx as nx

        with patch.object(
            nx, "eigenvector_centrality", side_effect=nx.PowerIterationFailedConvergence(1000)
        ):
            results = analyzer.get_centrality(metric="eigenvector")
            # Should return results with 0.0 eigenvector scores
            assert len(results) > 0
            for r in results:
                assert r.eigenvector == 0.0


# ═══════════════════════════════════════════════════════════════════
# Communities
# ═══════════════════════════════════════════════════════════════════


class TestCommunities:
    def test_detects_communities(self, analyzer):
        communities = analyzer.get_communities(min_size=2)
        assert len(communities) > 0
        assert isinstance(communities[0], Community)
        # Total members across all communities should cover connected nodes
        all_members = set()
        for c in communities:
            for m in c.members:
                all_members.add(m["entity_id"])
        assert len(all_members) >= 2

    def test_sorted_by_size(self, analyzer):
        communities = analyzer.get_communities(min_size=2)
        if len(communities) > 1:
            assert communities[0].size >= communities[1].size

    def test_min_size_filter(self, analyzer):
        # Set min_size very high to exclude all
        communities = analyzer.get_communities(min_size=100)
        assert communities == []

    def test_type_filter(self, analyzer):
        communities = analyzer.get_communities(entity_type="person", min_size=2)
        for c in communities:
            for m in c.members:
                assert m["type"] == "person"

    def test_empty_graph(self, empty_db):
        a = GraphAnalyzer(empty_db)
        communities = a.get_communities()
        assert communities == []


# ═══════════════════════════════════════════════════════════════════
# Shortest Path
# ═══════════════════════════════════════════════════════════════════


class TestShortestPath:
    def test_direct_connection(self, analyzer):
        result = analyzer.find_shortest_path(1, 2)  # Alice-Bob
        assert result is not None
        assert result.hops == 1
        assert result.total_weight == 7  # merged weight
        assert len(result.nodes) == 2
        assert len(result.edges) == 1

    def test_multi_hop(self, analyzer):
        result = analyzer.find_shortest_path(1, 5)  # Alice → ... → NYC
        assert result is not None
        assert result.hops >= 1
        assert len(result.nodes) == result.hops + 1

    def test_no_path_not_in_graph(self, analyzer):
        """Frank (6) is isolated (not in graph) — no path to Alice (1)."""
        result = analyzer.find_shortest_path(1, 6)
        assert result is None

    def test_no_path_disconnected_components(self, graph_db):
        """Two nodes in graph but in different components — NetworkXNoPath."""
        # Add a disconnected edge Frank-Eve so both are in the graph
        graph_db.execute(
            "INSERT INTO entities (id, name, type, canonical) VALUES (?, ?, ?, ?)",
            (8, "Eve", "person", "eve"),
        )
        graph_db.execute(
            "INSERT INTO entity_connections (entity_a_id, entity_b_id, weight) VALUES (?, ?, ?)",
            (6, 8, 1),
        )
        graph_db.commit()
        a = GraphAnalyzer(graph_db)
        result = a.find_shortest_path(1, 6)  # Alice(component1) → Frank(component2)
        assert result is None

    def test_nonexistent_entity(self, analyzer):
        result = analyzer.find_shortest_path(1, 999)
        assert result is None

    def test_same_entity(self, analyzer):
        result = analyzer.find_shortest_path(1, 1)
        assert result is not None
        assert result.hops == 0
        assert result.total_weight == 0
        assert len(result.nodes) == 1

    def test_path_result_structure(self, analyzer):
        result = analyzer.find_shortest_path(1, 2)
        assert isinstance(result, PathResult)
        assert "entity_id" in result.nodes[0]
        assert "name" in result.nodes[0]
        assert "type" in result.nodes[0]
        assert "source" in result.edges[0]
        assert "target" in result.edges[0]
        assert "weight" in result.edges[0]


# ═══════════════════════════════════════════════════════════════════
# Neighbors
# ═══════════════════════════════════════════════════════════════════


class TestNeighbors:
    def test_one_hop(self, analyzer):
        neighbors = analyzer.get_neighbors(1)  # Alice
        # Alice connects to: Bob (7), Acme (2)
        assert len(neighbors) == 2
        ids = {n["entity_id"] for n in neighbors}
        assert 2 in ids  # Bob
        assert 4 in ids  # Acme

    def test_two_hops(self, analyzer):
        neighbors = analyzer.get_neighbors(1, hops=2)
        # Hop 1: Bob, Acme. Hop 2: Carol (via Bob), NYC (via Acme)
        ids = {n["entity_id"] for n in neighbors}
        assert 3 in ids  # Carol
        assert 5 in ids  # NYC
        assert len(neighbors) == 4

    def test_min_weight_filter(self, analyzer):
        neighbors = analyzer.get_neighbors(1, min_weight=3)
        # Only Bob (weight 7) passes — Acme (weight 2) filtered out
        assert len(neighbors) == 1
        assert neighbors[0]["entity_id"] == 2

    def test_isolated_node(self, analyzer):
        neighbors = analyzer.get_neighbors(6)  # Frank — isolated
        assert neighbors == []

    def test_nonexistent_node(self, analyzer):
        neighbors = analyzer.get_neighbors(999)
        assert neighbors == []

    def test_sorted_by_weight(self, analyzer):
        neighbors = analyzer.get_neighbors(1)
        weights = [n["weight"] for n in neighbors]
        assert weights == sorted(weights, reverse=True)

    def test_hop_attribute(self, analyzer):
        neighbors = analyzer.get_neighbors(1, hops=2)
        hop1 = [n for n in neighbors if n["hop"] == 1]
        hop2 = [n for n in neighbors if n["hop"] == 2]
        assert len(hop1) == 2
        assert len(hop2) == 2


# ═══════════════════════════════════════════════════════════════════
# Subgraph
# ═══════════════════════════════════════════════════════════════════


class TestSubgraph:
    def test_subset_extraction(self, analyzer):
        result = analyzer.get_subgraph([1, 2, 3])
        assert len(result["nodes"]) == 3
        # Edges between these: A-B and B-C
        assert len(result["edges"]) == 2

    def test_empty_list(self, analyzer):
        result = analyzer.get_subgraph([])
        assert result == {"nodes": [], "edges": []}

    def test_nonexistent_ids(self, analyzer):
        result = analyzer.get_subgraph([998, 999])
        assert result == {"nodes": [], "edges": []}

    def test_single_node(self, analyzer):
        result = analyzer.get_subgraph([1])
        assert len(result["nodes"]) == 1
        assert len(result["edges"]) == 0

    def test_node_structure(self, analyzer):
        result = analyzer.get_subgraph([1])
        node = result["nodes"][0]
        assert "entity_id" in node
        assert "name" in node
        assert "type" in node


# ═══════════════════════════════════════════════════════════════════
# Import Guard
# ═══════════════════════════════════════════════════════════════════


class TestImportGuard:
    def test_missing_networkx_raises(self):
        """Simulate networkx being unavailable."""
        import dossier.core.graph_analysis as mod

        with patch.dict("sys.modules", {"networkx": None}):
            importlib.reload(mod)
            assert mod.HAS_NETWORKX is False
            with pytest.raises(ImportError, match="networkx is required"):
                mod._require_networkx()

        # Restore
        importlib.reload(mod)
        assert mod.HAS_NETWORKX is True
