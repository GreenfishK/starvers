import pytest
import uuid
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from app.main import app
from app.Database import get_session
from app.models.KnowledgeGraphModel import KnowledgeGraph


@pytest.fixture(name="session")
def session_fixture():
    engine = create_engine(
        "sqlite://", connect_args={"check_same_thread": False}, poolclass=StaticPool
    )
    SQLModel.metadata.create_all(engine)
    with Session(engine) as session:
        yield session


@pytest.fixture(name="client")
def client_fixture(session: Session):
    def get_session_override():
        return session

    app.dependency_overrides[get_session] = get_session_override
    client = TestClient(app)
    yield client
    app.dependency_overrides.clear()


def test_get_all_knowledge_graphs(session: Session, client: TestClient):
    kg_active = __get_test_data_active()
    kg_inactive = __get_test_data_inactive()
    session.add(kg_active)
    session.add(kg_inactive)
    session.commit()

    response = client.get("/management/")
    data = response.json()

    assert response.status_code == 200

    assert len(data) == 1
    __assert_equals(data[0], kg_active)

def test_get_all_knowledge_graphs_empty(session: Session, client: TestClient):
    kg_inactive = __get_test_data_inactive()
    session.add(kg_inactive)
    session.commit()

    response = client.get("/management/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 0


def test_get_knowledge_graph_by_id_active(session: Session, client: TestClient):
    kg_active = __get_test_data_active()
    session.add(kg_active)
    session.commit()

    response = client.get(f"/management/{kg_active.id}")
    data = response.json()

    assert response.status_code == 200
    __assert_equals(data, kg_active)

def test_get_knowledge_graph_by_id_inactive(session: Session, client: TestClient):
    kg_inactive = __get_test_data_inactive()
    session.add(kg_inactive)
    session.commit()

    response = client.get(f"/management/{kg_inactive.id}")
    data = response.json()

    assert response.status_code == 200
    __assert_equals(data, kg_inactive)

def test_get_knowledge_graph_by_id_unknown_uuid(session: Session, client: TestClient):
    kg_active = __get_test_data_active()
    session.add(kg_active)
    session.commit()

    unknownUuid = uuid.uuid4()

    response = client.get(f"/management/{unknownUuid}")
    data = response.json()

    assert response.status_code == 404
    assert data['message'] == f"Oops! Knowledge Graph with id {unknownUuid} not found!"


def test_start_knowledge_graph_versioning(session: Session, client: TestClient):
    kg_active = __get_test_data_active()

    response = client.post("/management/", json={
        "name": kg_active.name,
        "ressource_url": kg_active.ressource_url,
        "poll_interval": kg_active.poll_interval
    })
    data = response.json()

    # TODO check if run method works like expected

    assert response.status_code == 201
    __assert_equals(data, kg_active)

def test_start_knowledge_graph_versioning_invalid(session: Session, client: TestClient):
    response = client.post("/management/", json={})

    assert response.status_code == 422

def test_stop_knowledge_graph_versioning_by_id(session: Session, client: TestClient):
    kg_active = __get_test_data_active()
    session.add(kg_active)
    session.commit()

    response = client.delete(f"/management/{kg_active.id}")
    data = response.json()

    print(data)
    print(kg_active)

    assert response.status_code == 200
    __assert_equals(data, kg_active)

def test_stop_knowledge_graph_versioning_by_id_inactive(session: Session, client: TestClient):
    kg_inactive = __get_test_data_inactive()
    session.add(kg_inactive)
    session.commit()

    response = client.delete(f"/management/{kg_inactive.id}")
    data = response.json()

    assert response.status_code == 200
    __assert_equals(data, kg_inactive)

def test_stop_knowledge_graph_versioning_by_id_unknown(session: Session, client: TestClient):
    kg_active = __get_test_data_active()
    session.add(kg_active)
    session.commit()

    unknownUuid = uuid.uuid4()

    response = client.delete(f"/management/{unknownUuid}")
    data = response.json()

    assert response.status_code == 404
    assert data['message'] == f"Oops! Knowledge Graph with id {unknownUuid} not found!"

def __get_test_data_active() -> KnowledgeGraph:
    return KnowledgeGraph(name="Test Active", ressource_url="www.test-active.at", poll_interval=1000)

def __get_test_data_inactive() -> KnowledgeGraph:
    return KnowledgeGraph(name="Test Inactive", ressource_url="www.test-inactive.at", poll_interval=1000, active=False)

def __assert_equals(actual: any, expected: KnowledgeGraph, id_unknown: bool = False):
    if id_unknown:
        assert actual["id"] == str(expected.id)
    else:
        assert actual["id"] is not None
    assert actual["name"] == expected.name
    assert actual["ressource_url"] == expected.ressource_url
    assert actual["poll_interval"] == expected.poll_interval
    assert actual["active"] == expected.active