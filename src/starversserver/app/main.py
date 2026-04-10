import random
import string
import time

import uvicorn
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.persistance.Database import Session, engine, create_db_and_tables
from app.LoggingConfig import get_logger, setup_logging
from app.api import mock_router, management_router, query_router
from app.models.DeltaEventModel import DeltaEvent
from app.services.tracking_service import restart_active_tracking_tasks
from app.exceptions.DatasetNotFoundException import DatasetNotFoundException
from app.exceptions.RepositoryCreationFailedException import GraphRepositoryCreationFailedException
from app.exceptions.ServerFileImportFailedException import ServerFileImportFailedException

LOG = get_logger(__name__)


# ---------------------------------------------------------------------------
# Application lifespan: startup and shutdown hooks
# ---------------------------------------------------------------------------

@asynccontextmanager
async def lifespan(app: FastAPI):
    setup_logging()
    LOG.info("Creating database tables...")
    create_db_and_tables()

    LOG.info("Starting mock service...")
    mock_router.tl.start()

    # Re-schedule any datasets that were active before a restart
    with Session(engine) as session:
        restart_active_tracking_tasks(session)

    yield

    mock_router.tl.stop()


# ---------------------------------------------------------------------------
# FastAPI app setup
# ---------------------------------------------------------------------------

app = FastAPI(lifespan=lifespan)
app.openapi_tags = [management_router.tag_metadata]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(management_router.router)
app.include_router(query_router.router)
app.include_router(mock_router.router)


# ---------------------------------------------------------------------------
# Request logging middleware
# ---------------------------------------------------------------------------

@app.middleware("http")
async def log_requests(request: Request, call_next):
    request_id = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    LOG.info(f"rid={request_id} start path={request.url.path}")

    start_time = time.time()
    response = await call_next(request)
    elapsed_ms = '{0:.2f}'.format((time.time() - start_time) * 1000)

    LOG.info(f"rid={request_id} completed_in={elapsed_ms}ms status={response.status_code}")
    return response


# ---------------------------------------------------------------------------
# Exception handlers
# ---------------------------------------------------------------------------

@app.exception_handler(DatasetNotFoundException)
async def handle_dataset_not_found(request: Request, exc: DatasetNotFoundException):
    return JSONResponse(status_code=404, content={"message": f"Dataset with id {exc.id} not found."})


@app.exception_handler(GraphRepositoryCreationFailedException)
async def handle_graph_creation_failed(request: Request, exc: GraphRepositoryCreationFailedException):
    return JSONResponse(status_code=400, content={"message": f"Failed to create repository '{exc.repository_name}': {exc.error}"})


@app.exception_handler(ServerFileImportFailedException)
async def handle_serverfile_import_failed(request: Request, exc: ServerFileImportFailedException):
    return JSONResponse(status_code=400, content={"message": f"Failed to import server file for repository '{exc.repository_name}': {exc.error}"})


# ---------------------------------------------------------------------------
# Webhook documentation stub
# ---------------------------------------------------------------------------

@app.webhooks.post("delta-event")
def delta_event_notification(body: DeltaEvent):
    """
    Subscribers receive a POST to their registered URL each time a delta is calculated.
    The payload contains enough information to query the corresponding RDF dataset.
    """


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)