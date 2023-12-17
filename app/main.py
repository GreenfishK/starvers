from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from contextlib import asynccontextmanager

from app.api import management_rest_service, query_rest_service
from app.database import create_db_and_tables
from app.services.knowledge_graph_management import KnowledgeGraphNotFoundException

@asynccontextmanager
async def lifespan(app: FastAPI):
    create_db_and_tables()
    yield
    # optional action after terminatin application here

app = FastAPI(lifespan=lifespan);

app.include_router(management_rest_service.router);
app.include_router(query_rest_service.router);

@app.exception_handler(KnowledgeGraphNotFoundException)
async def knowledge_graph_not_found_exception_handler(request: Request, exc: KnowledgeGraphNotFoundException):
    return JSONResponse(
        status_code=404,
        content={"message": f"Oops! Knowledge Graph with id {exc.id} not found!"},
    )