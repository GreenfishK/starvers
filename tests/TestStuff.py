from datetime import datetime

import requests
from app.enums.DeltaTypeEnum import DeltaType
from app.models.DeltaEventModel import DeltaEvent
from app.services.DeltaCalculationService import IterativeDeltaQueryService, SparqlDeltaQueryService
from starvers.starvers import TripleStoreEngine

from app.utils.HelperService import convert_df_to_n3
from app.utils.exceptions.RepositoryCreationFailedException import GraphRepositoryCreationFailedException
from app.utils.graphdb.GraphDatabaseUtils import create_repository


def test_IterativeDeltaQueryService():
    try:
        create_repository("test_iterative")
    except GraphRepositoryCreationFailedException:
        pass

    starvers_engine = TripleStoreEngine("http://localhost:7200/repositories/test_iterative", "http://localhost:7200/repositories/test_iterative/statements")

    delta_service = IterativeDeltaQueryService(starvers_engine, "https://orkg.org/api/rdf/dump", "test_iterative_tmp")
    
    version_timestamp = datetime.now()
    delta_service.load_rdf_data()
    starvers_engine.version_all_triples(initial_timestamp=version_timestamp)

    delta_service.prepare()

    insertions, deletes = delta_service.calculate_delta(version_timestamp)

    starvers_engine.insert(convert_df_to_n3(insertions))
    starvers_engine.outdate(convert_df_to_n3(deletes))
    insertions_after_versioning, deletes_after_versioning = delta_service.calculate_delta()

    delta_service.clean_up()

    assert len(insertions) >= 0
    assert len(deletes) >= 0
    assert len(insertions_after_versioning) >= 0
    assert len(deletes_after_versioning) >= 0


def test_SparqlDeltaQueryService():
    create_repository("test_sparql")
    starvers_engine = TripleStoreEngine("http://localhost:7200/repositories/test_sparql", "http://localhost:7200/repositories/test_sparql/statements")
    starvers_engine_tmp = TripleStoreEngine("http://localhost:7200/repositories/test_sparql_tmp", "http://localhost:7200/repositories/test_sparql_tmp/statements")

    delta_service = SparqlDeltaQueryService(starvers_engine, starvers_engine_tmp, "test_sparql_tmp")
    delta_service.prepare()


    insertions, deletes = delta_service.calculate_delta()

    starvers_engine.insert(convert_df_to_n3(insertions))
    starvers_engine.outdate(convert_df_to_n3(deletes))
    insertions_after_versioning, deletes_after_versioning = delta_service.calculate_delta()

    delta_service.clean_up()
    # delete_repository("test_sparql")

    assert len(insertions) >= 0
    assert len(deletes) >= 0
    assert len(insertions_after_versioning) >= 0
    assert len(deletes_after_versioning) >= 0

def test_webhook():
    data = DeltaEvent(
        id="dc938535-ad5c-4440-92fb-731cc035e7b5",
        name="name",
        repository_name="repository_name",
        delta_type=DeltaType.SPARQL,
        totalInsertions=123,
        totalDeletions=123,
        insertions=["a", "b", "c"],
        deletions=["a", "b", "c"],
        versioning_duration_ms=23456,
        timestamp=datetime.now()
    )
    
    response = requests.post("https://starvers.free.beeceptor.com/diff-event", json=data.model_dump(mode='json'))
    assert response.status_code == 200