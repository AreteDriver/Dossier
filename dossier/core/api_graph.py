"""DOSSIER — Entity Graph Analysis API endpoints."""

from typing import Optional

from fastapi import APIRouter, HTTPException, Query

router = APIRouter()


def _get_db():
    from dossier.db.database import get_db

    return get_db


@router.get("/stats")
def graph_stats(type: Optional[str] = Query(None, description="Filter by entity type")):
    """Overall network statistics."""
    from dossier.core.graph_analysis import GraphAnalyzer

    get_db = _get_db()
    with get_db() as conn:
        analyzer = GraphAnalyzer(conn)
        stats = analyzer.get_stats(entity_type=type)
    return {
        "node_count": stats.node_count,
        "edge_count": stats.edge_count,
        "density": stats.density,
        "components": stats.components,
        "avg_degree": stats.avg_degree,
        "avg_weighted_degree": stats.avg_weighted_degree,
    }


@router.get("/centrality")
def graph_centrality(
    metric: str = Query("degree", description="degree|betweenness|closeness|eigenvector"),
    type: Optional[str] = Query(None, description="Filter by entity type"),
    limit: int = Query(50, ge=1, le=500),
):
    """Top entities by centrality metric."""
    from dossier.core.graph_analysis import GraphAnalyzer

    get_db = _get_db()
    with get_db() as conn:
        analyzer = GraphAnalyzer(conn)
        try:
            results = analyzer.get_centrality(metric=metric, entity_type=type, limit=limit)
        except ValueError as e:
            raise HTTPException(400, str(e))
    return {
        "metric": metric,
        "results": [
            {
                "entity_id": r.entity_id,
                "name": r.name,
                "type": r.type,
                "degree": r.degree,
                "weighted_degree": r.weighted_degree,
                "score": getattr(r, metric),
            }
            for r in results
        ],
    }


@router.get("/communities")
def graph_communities(
    type: Optional[str] = Query(None, description="Filter by entity type"),
    min_size: int = Query(2, ge=1),
):
    """Detect communities via Louvain method."""
    from dossier.core.graph_analysis import GraphAnalyzer

    get_db = _get_db()
    with get_db() as conn:
        analyzer = GraphAnalyzer(conn)
        communities = analyzer.get_communities(entity_type=type, min_size=min_size)
    return {
        "communities": [
            {
                "id": c.id,
                "members": c.members,
                "size": c.size,
                "density": c.density,
            }
            for c in communities
        ],
    }


@router.get("/path")
def graph_path(
    source_id: int = Query(..., description="Source entity ID"),
    target_id: int = Query(..., description="Target entity ID"),
):
    """Shortest path between two entities."""
    from dossier.core.graph_analysis import GraphAnalyzer

    get_db = _get_db()
    with get_db() as conn:
        analyzer = GraphAnalyzer(conn)
        result = analyzer.find_shortest_path(source_id, target_id)
    if result is None:
        raise HTTPException(404, "No path found between the specified entities")
    return {
        "nodes": result.nodes,
        "edges": result.edges,
        "total_weight": result.total_weight,
        "hops": result.hops,
    }


@router.get("/neighbors/{entity_id}")
def graph_neighbors(
    entity_id: int,
    hops: int = Query(1, ge=1, le=5),
    min_weight: int = Query(1, ge=1),
):
    """Neighborhood of an entity within N hops."""
    from dossier.core.graph_analysis import GraphAnalyzer

    get_db = _get_db()
    with get_db() as conn:
        analyzer = GraphAnalyzer(conn)
        neighbors = analyzer.get_neighbors(entity_id, hops=hops, min_weight=min_weight)
        if not neighbors:
            # Check if entity exists at all
            row = conn.execute("SELECT id FROM entities WHERE id = ?", (entity_id,)).fetchone()
            if not row:
                raise HTTPException(404, f"Entity {entity_id} not found")
    return {"entity_id": entity_id, "neighbors": neighbors}


@router.get("/subgraph")
def graph_subgraph(
    entity_ids: str = Query("", description="Comma-separated entity IDs"),
):
    """Induced subgraph for given entity IDs."""
    from dossier.core.graph_analysis import GraphAnalyzer

    if not entity_ids.strip():
        return {"nodes": [], "edges": []}

    ids = [int(x.strip()) for x in entity_ids.split(",") if x.strip()]

    get_db = _get_db()
    with get_db() as conn:
        analyzer = GraphAnalyzer(conn)
        result = analyzer.get_subgraph(ids)
    return result
