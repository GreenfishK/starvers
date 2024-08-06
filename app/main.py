import random
import string
import time
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from app.Database import Session, engine

from app.api import ManagementRestService, MockRestService, QueryRestService
from app.Database import create_db_and_tables
from app.models.DeltaEventModel import DeltaEvent
from app.services.ManagementService import restart
from app.utils.exceptions.GraphNotFoundException import KnowledgeGraphNotFoundException
from app.utils.exceptions.RepositoryCreationFailedException import GraphRepositoryCreationFailedException
import uvicorn

import logging

LOG = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    MockRestService.tl.start()

    with Session(engine) as session:
        restart(session)
    yield

    # optional action after terminating application here
    MockRestService.tl.stop()


app = FastAPI(lifespan=lifespan)

app.openapi_tags = [ManagementRestService.tag_metadata]

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

@app.exception_handler(KnowledgeGraphNotFoundException)
async def knowledge_graph_not_found_exception_handler(request: Request, exc: KnowledgeGraphNotFoundException):
    return JSONResponse(
        status_code=404,
        content={"message": f"Oops! Knowledge Graph with id {exc.id} not found!"},
    )

@app.exception_handler(GraphRepositoryCreationFailedException)
async def graph_creation_failed_exception_handler(request: Request, exc: GraphRepositoryCreationFailedException):
    return JSONResponse(
        status_code=400,
        content={"message": f"Oops! Creation for Repository with name {exc.reposotory_name}! [{exc.error}]"},
    )

@app.webhooks.post("delta-event")
def delta_event_notification(body: DeltaEvent):
    """
    When someone subscribes to the DeltaQueryService results, the service will send a POST request to the registered URL
    every time a delta was calculated. The request payload contains relevant data to be able to query the corresponding knowledge graph.
    """

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000, log_config='log_config.yaml')