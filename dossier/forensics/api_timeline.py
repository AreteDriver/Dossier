"""
DOSSIER — Timeline API Endpoints

Mount these into the existing FastAPI app:
    from dossier.forensics.api_timeline import router as timeline_router
    app.include_router(timeline_router, prefix="/api/timeline", tags=["timeline"])
"""

from fastapi import APIRouter, Query
from typing import Optional

router = APIRouter()


def _get_db():
    """Import at call time to avoid circular imports with existing DOSSIER db module."""
    from dossier.db.database import get_db

    return get_db


@router.get("")
def get_timeline(
    start: Optional[str] = Query(None, description="Start date (ISO format, e.g. 2001-01-01)"),
    end: Optional[str] = Query(None, description="End date (ISO format)"),
    entity: Optional[str] = Query(None, description="Filter by entity name"),
    document_id: Optional[int] = Query(None, description="Filter by document ID"),
    min_confidence: float = Query(0.0, ge=0.0, le=1.0, description="Minimum confidence threshold"),
    include_unresolved: bool = Query(False, description="Include unresolved relative dates"),
    limit: int = Query(200, ge=1, le=1000),
):
    """Query the reconstructed timeline with filters."""
    from dossier.forensics.timeline import query_timeline

    get_db = _get_db()
    with get_db() as conn:
        events = query_timeline(
            conn,
            start_date=start,
            end_date=end,
            entity_name=entity,
            document_id=document_id,
            min_confidence=min_confidence,
            include_unresolved=include_unresolved,
            limit=limit,
        )
    return {"events": events, "count": len(events)}


@router.get("/stats")
def timeline_stats():
    """Get timeline summary statistics."""
    from dossier.forensics.timeline import get_timeline_stats

    get_db = _get_db()
    with get_db() as conn:
        stats = get_timeline_stats(conn)
    return stats


@router.get("/unresolved")
def unresolved_dates(
    limit: int = Query(50, ge=1, le=500),
):
    """
    Get unresolved relative dates that need manual review.
    These are dates like 'the following Tuesday' or 'two weeks later'
    that couldn't be automatically resolved.
    """
    from dossier.forensics.timeline import query_timeline

    get_db = _get_db()
    with get_db() as conn:
        events = query_timeline(
            conn,
            include_unresolved=True,
            min_confidence=0.0,
            limit=limit,
        )
        # Filter to only unresolved
        unresolved = [e for e in events if not e.get("is_resolved", True)]
    return {"unresolved": unresolved, "count": len(unresolved)}


@router.post("/extract/{document_id}")
def extract_timeline_for_document(document_id: int):
    """
    Run timeline extraction on a specific document.
    Useful for re-processing or processing documents ingested before
    the timeline module was added.
    """
    from dossier.forensics.timeline import TimelineExtractor, store_events, init_timeline_tables

    get_db = _get_db()
    with get_db() as conn:
        # Get document text
        doc = conn.execute(
            "SELECT id, raw_text FROM documents WHERE id = ?", (document_id,)
        ).fetchone()

        if not doc:
            return {"error": f"Document {document_id} not found"}, 404

        # Get known entities for linking
        entity_rows = conn.execute("SELECT name FROM entities").fetchall()
        entity_names = [r["name"] for r in entity_rows]

        # Ensure timeline tables exist
        init_timeline_tables(conn)

        # Clear existing events for this document (re-extraction)
        conn.execute("DELETE FROM events WHERE document_id = ?", (document_id,))

        # Extract
        extractor = TimelineExtractor(entity_names=entity_names)
        events = extractor.extract_events(doc["raw_text"], document_id=document_id)

        # Store
        event_ids = store_events(conn, events)

    return {
        "document_id": document_id,
        "events_extracted": len(events),
        "resolved": sum(1 for e in events if e.is_resolved),
        "unresolved": sum(1 for e in events if not e.is_resolved),
        "event_ids": event_ids,
    }


@router.post("/extract-all")
def extract_timeline_for_all_documents():
    """
    Run timeline extraction across the entire corpus.
    Clears and rebuilds the entire events table.
    """
    from dossier.forensics.timeline import TimelineExtractor, store_events, init_timeline_tables

    get_db = _get_db()
    with get_db() as conn:
        init_timeline_tables(conn)

        # Get all known entities for linking
        entity_rows = conn.execute("SELECT name FROM entities").fetchall()
        entity_names = [r["name"] for r in entity_rows]

        # Clear all events
        conn.execute("DELETE FROM events")
        conn.execute("DELETE FROM event_entities")

        extractor = TimelineExtractor(entity_names=entity_names)

        # Process each document
        docs = conn.execute("SELECT id, raw_text FROM documents").fetchall()
        total_events = 0
        total_unresolved = 0

        for doc in docs:
            events = extractor.extract_events(doc["raw_text"], document_id=doc["id"])
            store_events(conn, events)
            total_events += len(events)
            total_unresolved += sum(1 for e in events if not e.is_resolved)

    return {
        "documents_processed": len(docs),
        "total_events": total_events,
        "resolved": total_events - total_unresolved,
        "unresolved": total_unresolved,
    }
