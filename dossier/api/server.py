"""
DOSSIER — FastAPI Backend
REST API for the Document Intelligence System.

Thin orchestration layer — routes live in routes_*.py modules.
"""

import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware

from dossier.db.database import init_db
from dossier.forensics.api_timeline import router as timeline_router
from dossier.core.api_resolver import router as resolver_router
from dossier.core.api_graph import router as graph_router
from dossier.api import utils
from dossier.api.routes_ingestion import router as ingestion_router
from dossier.api.routes_search import router as search_router
from dossier.api.routes_documents import router as documents_router
from dossier.api.routes_entities import router as entities_router
from dossier.api.routes_forensics import router as forensics_router
from dossier.api.routes_collaboration import router as collaboration_router
from dossier.api.routes_investigation import router as investigation_router
from dossier.api.routes_intelligence import router as intelligence_router
from dossier.api.routes_analytics import router as analytics_router

logger = logging.getLogger(__name__)

app = FastAPI(title="DOSSIER", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Pre-existing routers ────────────────────────────────────────
app.include_router(timeline_router, prefix="/api/timeline", tags=["timeline"])
app.include_router(resolver_router, prefix="/api/resolver", tags=["resolver"])
app.include_router(graph_router, prefix="/api/graph", tags=["graph"])

# ── Decomposed routers ─────────────────────────────────────────
app.include_router(ingestion_router, prefix="/api", tags=["ingestion"])
app.include_router(search_router, prefix="/api", tags=["search"])
app.include_router(documents_router, prefix="/api", tags=["documents"])
app.include_router(entities_router, prefix="/api", tags=["entities"])
app.include_router(forensics_router, prefix="/api", tags=["forensics"])
app.include_router(collaboration_router, prefix="/api", tags=["collaboration"])
app.include_router(investigation_router, prefix="/api", tags=["investigation"])
app.include_router(intelligence_router, prefix="/api", tags=["intelligence"])
app.include_router(analytics_router, prefix="/api", tags=["analytics"])


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Catch-all handler to prevent stack traces from leaking to clients."""
    logger.exception("Unhandled error on %s %s", request.method, request.url.path)
    return JSONResponse(
        status_code=500,
        content={"detail": "Internal server error"},
    )


@app.on_event("startup")
def startup():
    init_db()
    utils.UPLOAD_DIR.mkdir(parents=True, exist_ok=True)


# ── Static files (serve the frontend) ──────────────────────────

STATIC_DIR = Path(__file__).parent.parent / "static"


@app.get("/")
def serve_frontend():
    index = STATIC_DIR / "index.html"
    if index.exists():
        return FileResponse(index)
    return {"message": "DOSSIER API is running. Place index.html in /static to serve the frontend."}
