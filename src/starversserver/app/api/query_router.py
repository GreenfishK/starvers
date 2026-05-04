"""
query_router.py

REST endpoints for executing SPARQL queries against versioned RDF datasets.
Supports querying the latest version or any past snapshot via an optional timestamp.
"""

from asyncio.log import logger
from datetime import datetime
from io import BytesIO
from turtle import pd
from typing import Annotated
from uuid import UUID
from flask import send_file

from fastapi import APIRouter, Body, Query

from app.utils.starvers.starvers import TripleStoreEngine
from app.AppConfig import Settings

tag = "query"

router = APIRouter(prefix="/query", tags=[tag])

tag_metadata = {
    "name": tag,
    "description": "Execute SPARQL queries against versioned datasets — at the latest version or at a specific point in time.",
}


@router.get("/{repo_name}")
async def query_dataset(
    repo_name: str,
    query: Annotated[str, Body()],
    timestamp: Annotated[datetime | None, Query()] = None,
    query_as_timestamped: Annotated[bool, Query()] = True,
):
    """
    Execute a SPARQL query against a tracked dataset 
    and returns the timestamped query as an persistent identifier and the result set.
    . **repo_name**: the name of the dataset repository.
    - **query**: the SPARQL query to execute.
    - **timestamp**: if provided, the query is evaluated against the snapshot at that point in time.
    - **query_as_timestamped**: when False the query runs without time-bounding (returns the raw graph).
    """
    graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', repo_name)
    graph_db_post_endpoint = Settings().graph_db_url_post_endpoint.replace('{:repo_name}', repo_name)
    starvers_engine = TripleStoreEngine(graph_db_get_endpoint, graph_db_post_endpoint)

    if timestamp is not None and query_as_timestamped:
        logger.info(f"Execute timestamped query with timestamp={timestamp}")
    else:
        logger.info("Execute query without timestamp")
    
    result_set_df = pd.DataFrame()
    timestamped_query = ""
    try:
        result_set_df = starvers_engine.query(query, timestamp, query_as_timestamped)
    except TimeoutError as e:
        # Does only catch timeout from the http request, not the database query itself, 
        # which can have a different timeout.
        raise Exception(f"Timeout of {starvers_engine.timeout} seconds exceeded: {e}")    
    except Exception as e:
        logger.error(f"An error occurred during query execution: {str(e)}")
        raise Exception(f"An error occurred during query execution: {str(e)}")

    logger.info(f"Result set contains {len(result_set_df)} records.")

    csv_string = result_set_df.to_csv(index=False)

    # Encode the string into bytes and wrap it with BytesIO
    csv_buffer = BytesIO(csv_string.encode("utf-8"))
    csv_buffer.seek(0)

    return send_file(csv_buffer, mimetype="text/csv", as_attachment=True, download_name="query_result.csv"), timestamped_query
    

