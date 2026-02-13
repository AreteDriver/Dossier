"""
DOSSIER — Entity Resolver API Endpoints

Mount these into the existing FastAPI app:
    from dossier.core.api_resolver import router as resolver_router
    app.include_router(resolver_router, prefix="/api/resolver", tags=["resolver"])
"""

from fastapi import APIRouter, HTTPException, Query
from typing import Optional

router = APIRouter()


def _get_db():
    """Import at call time to avoid circular imports with existing DOSSIER db module."""
    from dossier.db.database import get_db

    return get_db


@router.post("/resolve")
def resolve_all(entity_type: Optional[str] = Query(None, description="Filter by entity type")):
    """Run entity resolution across the corpus."""
    from dossier.core.resolver import EntityResolver

    get_db = _get_db()
    with get_db() as conn:
        resolver = EntityResolver(conn)
        result = resolver.resolve_all(entity_type=entity_type)

    return {
        "entities_scanned": result.entities_scanned,
        "auto_merged": result.auto_merged,
        "suggested": result.suggested,
        "skipped": result.skipped,
        "matches": [
            {
                "source_id": m.source_id,
                "source_name": m.source_name,
                "target_id": m.target_id,
                "target_name": m.target_name,
                "confidence": m.confidence,
                "strategy": m.strategy,
                "action": m.action.value,
            }
            for m in result.matches
        ],
    }


@router.post("/resolve/{entity_id}")
def resolve_single(entity_id: int):
    """Resolve a single entity against the corpus."""
    from dossier.core.resolver import EntityResolver

    get_db = _get_db()
    with get_db() as conn:
        resolver = EntityResolver(conn)
        matches = resolver.resolve_entity(entity_id)

    return {
        "entity_id": entity_id,
        "matches": [
            {
                "source_id": m.source_id,
                "source_name": m.source_name,
                "target_id": m.target_id,
                "target_name": m.target_name,
                "confidence": m.confidence,
                "strategy": m.strategy,
                "action": m.action.value,
            }
            for m in matches
        ],
    }


@router.get("/duplicates")
def get_duplicates():
    """Get all resolved duplicate pairs."""
    from dossier.core.resolver import EntityResolver

    get_db = _get_db()
    with get_db() as conn:
        resolver = EntityResolver(conn)
        dupes = resolver.get_duplicates()

    return {"duplicates": dupes}


@router.get("/queue")
def get_queue():
    """Get the human review queue."""
    get_db = _get_db()
    with get_db() as conn:
        rows = conn.execute(
            """
            SELECT rq.id, rq.source_entity_id, e1.name as source_name,
                   rq.target_entity_id, e2.name as target_name,
                   rq.confidence, rq.strategy, rq.created_at
            FROM resolution_queue rq
            JOIN entities e1 ON e1.id = rq.source_entity_id
            JOIN entities e2 ON e2.id = rq.target_entity_id
            ORDER BY rq.confidence DESC
        """
        ).fetchall()

    return {"queue": [dict(r) for r in rows]}


@router.post("/queue/{queue_id}/review")
def review_queue_item(queue_id: int, approve: bool = Query(...)):
    """Approve or reject a suggested merge from the review queue."""
    from dossier.core.resolver import EntityResolver

    get_db = _get_db()
    with get_db() as conn:
        resolver = EntityResolver(conn)
        success = resolver.review_queue_item(queue_id, approve)

    if not success:
        raise HTTPException(status_code=404, detail=f"Queue item {queue_id} not found")
    return {"queue_id": queue_id, "approved": approve}


@router.post("/merge")
def merge_entities(source_id: int = Query(...), target_id: int = Query(...)):
    """Manually merge two entities (target becomes canonical)."""
    from dossier.core.resolver import EntityResolver

    get_db = _get_db()
    with get_db() as conn:
        resolver = EntityResolver(conn)
        success = resolver.merge_entities(source_id, target_id)

    if not success:
        raise HTTPException(status_code=404, detail="One or both entities not found")
    return {"merged": True, "source_id": source_id, "target_id": target_id}


@router.post("/split")
def split_entities(source_id: int = Query(...), target_id: int = Query(...)):
    """Undo a merge — split an entity from its canonical."""
    from dossier.core.resolver import EntityResolver

    get_db = _get_db()
    with get_db() as conn:
        resolver = EntityResolver(conn)
        success = resolver.split_entity(source_id, target_id)

    if not success:
        raise HTTPException(status_code=404, detail="No resolution found for this pair")
    return {"split": True, "source_id": source_id, "target_id": target_id}


@router.get("/aliases/{entity_id}")
def get_aliases(entity_id: int):
    """Get all known aliases for an entity."""
    from dossier.core.resolver import EntityResolver

    get_db = _get_db()
    with get_db() as conn:
        resolver = EntityResolver(conn)
        aliases = resolver.get_aliases(entity_id)

    return {"entity_id": entity_id, "aliases": aliases}
