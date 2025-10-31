import random
import string
import time
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager

from app.persistance.Database import Session, engine, create_db_and_tables

from app.LoggingConfig import get_logger, setup_logging
from app.api import ManagementRestService, MockRestService, QueryRestService
from app.models.DeltaEventModel import DeltaEvent
from app.services.ManagementService import restart
from app.exceptions.DatasetNotFoundException import DatasetNotFoundException
import uvicorn

from app.exceptions.RepositoryCreationFailedException import GraphRepositoryCreationFailedException
from app.exceptions.ServerFileImportFailedException import ServerFileImportFailedException

LOG = get_logger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    LOG.info("Setting up logging...")
    setup_logging()

    LOG.info("Creating database tables...")
    create_db_and_tables()

    LOG.info("Starting mocking service")
    MockRestService.tl.start()

    with Session(engine) as session:
        restart(session)
    yield

    # optional action after terminating application here
    MockRestService.tl.stop()


app = FastAPI(lifespan=lifespan)

app.openapi_tags = [ManagementRestService.tag_metadata]

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(ManagementRestService.router)
app.include_router(QueryRestService.router)
app.include_router(MockRestService.router)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    idem = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    LOG.info(f"rid={idem} start request path={request.url.path}")
    start_time = time.time()
    
    response = await call_next(request)
    
    process_time = (time.time() - start_time) * 1000
    formatted_process_time = '{0:.2f}'.format(process_time)
    LOG.info(f"rid={idem} completed_in={formatted_process_time}ms status_code={response.status_code}")
    
    return response

@app.exception_handler(DatasetNotFoundException)
async def dataset_not_found_exception_handler(request: Request, exc: DatasetNotFoundException):
    return JSONResponse(
        status_code=404,
        content={"message": f"Oops! Dataset with id {exc.id} not found!"},
    )

@app.exception_handler(GraphRepositoryCreationFailedException)
async def graph_creation_failed_exception_handler(request: Request, exc: GraphRepositoryCreationFailedException):
    return JSONResponse(
        status_code=400,
        content={"message": f"Oops! Creation for Repository with name {exc.repository_name}! [{exc.error}]"},
    )

@app.exception_handler(ServerFileImportFailedException)
async def serverfile_import_failed_exception_handler(request: Request, exc: ServerFileImportFailedException):
    return JSONResponse(
        status_code=400,
        content={"message": f"Oops! Importing server file for Repository with name {exc.repository_name} failed! [{exc.error}]"},
    )

@app.webhooks.post("delta-event")
def delta_event_notification(body: DeltaEvent):
    """
    When someone subscribes to the DeltaQueryService results, the service will send a POST request to the registered URL
    every time a delta was calculated. The request payload contains relevant data to be able to query the corresponding rdf dataset.
    """

# for manual start via python main.py
if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)