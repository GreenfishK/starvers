"""
metrics_service.py

Computes and persists per-snapshot statistics (class counts, property counts,
static-core and version-oblivious triple counts) after each versioning cycle.

All SPARQL queries return CSV bytes which are parsed with pandas.
All results are written to the Postgres `snapshot` table via SQLModel.
"""

import yaml
import pandas as pd
from datetime import datetime
from io import StringIO
from typing import Optional
from uuid import UUID

from SPARQLWrapper import SPARQLWrapper
from sqlmodel import Session, select
from sqlmodel import delete as sqlmodel_delete

from app.models.DatasetModel import Dataset, Snapshot
from app.exceptions.DatasetNotFoundException import DatasetNotFoundException
from app.persistance.graphdb.GraphDatabaseUtils import (
    get_snapshot_classes_template,
    get_snapshot_properties_template,
    get_dataset_static_core_template,
    get_dataset_version_oblivious_template,
)
from app.LoggingConfig import get_logger

LOG = get_logger(__name__)

ONTOLOGY_CONFIG_PATH = "app/persistance/graphdb/ontology_config.yml"


class MetricsService:
    """
    Bridges the SPARQL engine and the relational database for metrics storage.
    One instance is created per polling cycle (stateless between cycles).
    """

    def __init__(self, sparql_engine: SPARQLWrapper, session: Session):
        self.sparql_engine = sparql_engine
        self.session = session

    # ---------------------------------------------------------------------------
    # Public: update methods called after each versioning run
    # ---------------------------------------------------------------------------

    def update_class_statistics(
        self,
        dataset_id: UUID,
        repo_name: str,
        snapshot_ts: datetime,
        snapshot_ts_prev: Optional[datetime] = None,
    ):
        """Query class-level metrics from GraphDB and insert them into the snapshot table."""
        LOG.info(f"[{repo_name}] Updating class metrics for ts={snapshot_ts}, prev={snapshot_ts_prev}.")

        # When no previous snapshot exists, compare the snapshot against itself
        effective_prev = snapshot_ts_prev or snapshot_ts
        query = get_snapshot_classes_template(ts_current=snapshot_ts, ts_prev=effective_prev)

        df = self._run_sparql_csv_query(query)

        snapshots = [
            Snapshot(
                dataset_id=dataset_id,
                snapshot_ts=snapshot_ts,
                snapshot_ts_prev=effective_prev,
                onto_class=row["onto_class"],
                onto_class_label=row["onto_class_label"]     if pd.notna(row["onto_class_label"])     else None,
                parent_onto_class=row["parent_onto_class"]   if pd.notna(row["parent_onto_class"])    else None,
                cnt_class_instances_current=row["cnt_class_instances_current"],
                cnt_class_instances_prev=row["cnt_class_instances_prev"],
                cnt_classes_added=row["cnt_classes_added"],
                cnt_classes_deleted=row["cnt_classes_deleted"],
            )
            for _, row in df.iterrows()
        ]

        self._persist_snapshots(snapshots, repo_name, metric_type="class")

    def update_property_statistics(
        self,
        dataset_id: UUID,
        repo_name: str,
        snapshot_ts: datetime,
        snapshot_ts_prev: Optional[datetime] = None,
    ):
        """Query property-level metrics from GraphDB and insert them into the snapshot table."""
        LOG.info(f"[{repo_name}] Updating property metrics for ts={snapshot_ts}, prev={snapshot_ts_prev}.")

        property_identifiers = self._load_property_identifiers(repo_name)
        effective_prev = snapshot_ts_prev or snapshot_ts
        query = get_snapshot_properties_template(
            ts_current=snapshot_ts,
            ts_prev=effective_prev,
            property_identifiers=property_identifiers,
        )

        df = self._run_sparql_csv_query(query)

        snapshots = [
            Snapshot(
                dataset_id=dataset_id,
                snapshot_ts=snapshot_ts,
                snapshot_ts_prev=effective_prev,
                onto_property=row["onto_property"],
                onto_property_label=row["onto_property_label"] if pd.notna(row["onto_property_label"]) else None,
                parent_property=row["parent_property"]          if pd.notna(row["parent_property"])     else None,
                cnt_property_instances_current=row["cnt_property_instances_current"],
                cnt_property_instances_prev=row["cnt_property_instances_prev"],
                cnt_properties_added=row["cnt_properties_added"],
                cnt_properties_deleted=row["cnt_properties_deleted"],
            )
            for _, row in df.iterrows()
        ]

        self._persist_snapshots(snapshots, repo_name, metric_type="property")

    def update_static_core_triples(self, dataset: Dataset, repo_name: str):
        """Query and persist the number of static-core triples for this dataset."""
        df = self._run_sparql_csv_query(get_dataset_static_core_template())
        value = df.at[0, "cnt_triples_static_core"]
        dataset.cnt_triples_static_core = int(value) if pd.notna(value) else -1

        LOG.info(f"[{repo_name}] Static core triples: {dataset.cnt_triples_static_core}.")
        self.session.commit()
        self.session.refresh(dataset)

    def update_version_oblivious_triples(self, dataset: Dataset, repo_name: str):
        """Query and persist the number of version-oblivious triples for this dataset."""
        df = self._run_sparql_csv_query(get_dataset_version_oblivious_template())
        value = df.at[0, "cnt_triples_version_oblivious"]
        dataset.cnt_triples_version_oblivious = int(value) if pd.notna(value) else -1

        LOG.info(f"[{repo_name}] Version-oblivious triples: {dataset.cnt_triples_version_oblivious}.")
        self.session.commit()
        self.session.refresh(dataset)

    # ---------------------------------------------------------------------------
    # Snapshot deletion helpers (used by admin/reset endpoints)
    # ---------------------------------------------------------------------------

    def delete_snapshots_from(self, repo_name: str, start_timestamp: datetime):
        """Delete all snapshot rows at or after the given timestamp for the given repo."""
        dataset_id = self._resolve_dataset_id(repo_name)
        self.session.exec(
            sqlmodel_delete(Snapshot)
            .where(Snapshot.dataset_id == dataset_id)
            .where(Snapshot.snapshot_ts >= start_timestamp)
        )
        self.session.commit()

    def delete_all_snapshots(self, repo_name: str):
        """Delete every snapshot row for the given repository."""
        dataset_id = self._resolve_dataset_id(repo_name)
        self.session.exec(sqlmodel_delete(Snapshot).where(Snapshot.dataset_id == dataset_id))
        self.session.commit()

    # ---------------------------------------------------------------------------
    # Private helpers
    # ---------------------------------------------------------------------------

    def _run_sparql_csv_query(self, query: str) -> pd.DataFrame:
        """Execute a SPARQL query and parse the CSV response into a DataFrame."""
        self.sparql_engine.setQuery(query)
        response = self.sparql_engine.query().convert()

        if not isinstance(response, bytes):
            raise ValueError("Unexpected SPARQL response format — expected CSV bytes.")

        return pd.read_csv(StringIO(response.decode("utf-8")))

    def _persist_snapshots(self, snapshots: list[Snapshot], repo_name: str, metric_type: str):
        """Bulk-insert snapshot rows and commit."""
        if not snapshots:
            LOG.warning(f"[{repo_name}] No {metric_type} metrics returned — nothing inserted.")
            return

        LOG.info(f"[{repo_name}] Inserting {len(snapshots)} {metric_type} metric rows into snapshot table.")
        self.session.add_all(snapshots)
        self.session.commit()
        for snap in snapshots:
            self.session.refresh(snap)

    def _resolve_dataset_id(self, repo_name: str) -> UUID:
        dataset_id = self.session.exec(
            select(Dataset.id).where(Dataset.repository_name == repo_name)
        ).first()
        if not dataset_id:
            raise DatasetNotFoundException(name=repo_name)
        return dataset_id

    @staticmethod
    def _load_property_identifiers(repo_name: str) -> str:
        """
        Return a comma-separated string of property URIs to track.
        Falls back to the 'default' config entry if the repo has no specific config.
        """
        with open(ONTOLOGY_CONFIG_PATH, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        identifiers = (
            config.get(repo_name, {}).get("propertyIdentifiers")
            or config["default"]["propertyIdentifiers"]
        )
        return ", ".join(identifiers)
