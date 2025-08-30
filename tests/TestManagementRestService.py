import pytest
import uuid
from fastapi.testclient import TestClient
from sqlmodel import Session, SQLModel, create_engine
from sqlmodel.pool import StaticPool

from app.main import app
from app.persistance.Database import get_session
from app.models.DatasetModel import Dataset


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


def test_get_all_datasets(session: Session, client: TestClient):
    dataset_active = __get_test_data_active()
    dataset_inactive = __get_test_data_inactive()
    session.add(dataset_active)
    session.add(dataset_inactive)
    session.commit()

    response = client.get("/management/")
    data = response.json()

    assert response.status_code == 200

    assert len(data) == 1
    __assert_equals(data[0], dataset_active)


def test_get_all_datasets_empty(session: Session, client: TestClient):
    dataset_inactive = __get_test_data_inactive()
    session.add(dataset_inactive)
    session.commit()

    response = client.get("/management/")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 0


def test_get_dataset_by_id_active(session: Session, client: TestClient):
    dataset_active = __get_test_data_active()
    session.add(dataset_active)
    session.commit()

    response = client.get(f"/management/{dataset_active.id}")
    data = response.json()

    assert response.status_code == 200
    __assert_equals(data, dataset_active)


def test_get_dataset_by_id_inactive(session: Session, client: TestClient):
    dataset_inactive = __get_test_data_inactive()
    session.add(dataset_inactive)
    session.commit()

    response = client.get(f"/management/{dataset_inactive.id}")
    data = response.json()

    assert response.status_code == 200
    __assert_equals(data, dataset_inactive)


def test_get_dataset_by_id_unknown_uuid(session: Session, client: TestClient):
    dataset_active = __get_test_data_active()
    session.add(dataset_active)
    session.commit()

    unknownUuid = uuid.uuid4()

    response = client.get(f"/management/{unknownUuid}")
    data = response.json()

    assert response.status_code == 404
    assert data["message"] == f"Oops! Dataset with id {unknownUuid} not found!"


def test_start_dataset_versioning(session: Session, client: TestClient):
    dataset_active = __get_test_data_active()

    response = client.post(
        "/management/",
        json={
            "name": dataset_active.name,
            "repository_name": dataset_active.repository_name,
            "rdf_dataset_url": dataset_active.rdf_dataset_url,
            "polling_interval": dataset_active.polling_interval,
            "notification_webhook": None,
        },
    )
    data = response.json()

    # TODO check if run method works like expected
    assert response.status_code == 201
    __assert_equals(data, dataset_active)


def test_start_dataset_versioning_invalid(session: Session, client: TestClient):
    response = client.post("/management/", json={})

    assert response.status_code == 422


def test_stop_dataset_versioning_by_id(session: Session, client: TestClient):
    dataset_active = __get_test_data_active()
    session.add(dataset_active)
    session.commit()

    response = client.delete(f"/management/{dataset_active.id}")
    data = response.json()

    assert response.status_code == 200
    __assert_equals(data, dataset_active)


def test_stop_dataset_versioning_by_id_inactive(session: Session, client: TestClient):
    dataset_inactive = __get_test_data_inactive()
    session.add(dataset_inactive)
    session.commit()

    response = client.delete(f"/management/{dataset_inactive.id}")
    data = response.json()

    assert response.status_code == 200
    __assert_equals(data, dataset_inactive)


def test_stop_dataset_versioning_by_id_unknown(session: Session, client: TestClient):
    dataset_active = __get_test_data_active()
    session.add(dataset_active)
    session.commit()

    unknownUuid = uuid.uuid4()

    response = client.delete(f"/management/{unknownUuid}")
    data = response.json()

    assert response.status_code == 404
    assert data["message"] == f"Oops! Dataset with id {unknownUuid} not found!"


def test_stop_all_dataset_versionings(session: Session, client: TestClient):
    dataset_active = __get_test_data_active()
    session.add(dataset_active)
    session.commit()

    response = client.delete("/management/all")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 1


def test_stop_all_dataset_versionings_none_active(session: Session, client: TestClient):
    dataset_inactive = __get_test_data_inactive()
    session.add(dataset_inactive)
    session.commit()

    response = client.delete("/management/all")
    data = response.json()

    assert response.status_code == 200
    assert len(data) == 0


def __get_test_data_active() -> Dataset:
    return Dataset(
        name="Test Active",
        repository_name="test_active",
        rdf_dataset_url="https://www.test-active.at/",
        polling_interval=1000,
    )


def __get_test_data_inactive() -> Dataset:
    return Dataset(
        name="Test Inactive",
        repository_name="test_active",
        rdf_dataset_url="https://www.test-inactive.at/",
        polling_interval=1000,
        active=False,
    )


def __assert_equals(actual: any, expected: Dataset, id_unknown: bool = False):
    if id_unknown:
        assert actual["id"] == str(expected.id)
    else:
        assert actual["id"] is not None
    assert actual["name"] == expected.name
    assert actual["repository_name"] == expected.repository_name
    assert actual["rdf_dataset_url"] == expected.rdf_dataset_url
    assert actual["polling_interval"] == expected.polling_interval
    assert actual["active"] == expected.active
