import pandas as pd
from datetime import datetime
from io import StringIO
from sqlmodel import Session, select
from sqlmodel import delete as sqlmodel_delete
from typing import Optional
from SPARQLWrapper import SPARQLWrapper
from uuid import UUID
import yaml

from app.models.DatasetModel import Dataset, Snapshot
from app.persistance.graphdb.GraphDatabaseUtils import get_snapshot_classes_template, \
  get_snapshot_properties_template, get_dataset_static_core_template, \
  get_dataset_version_oblivious_template
from app.LoggingConfig import get_logger
from app.exceptions.DatasetNotFoundException import DatasetNotFoundException


LOG = get_logger(__name__)

class MetricsService():

    def __init__(self, sparql_engine: SPARQLWrapper, session: Session):
        super().__init__()
        self.sparql_engine = sparql_engine
        self.session = session


    def delete_snapshot_metrics_by_dataset_id_and_ts(self, repo_name: str, start_timestamp: datetime):
        dataset_id_stmt = select(Dataset.id).where(Dataset.repository_name == repo_name)
        dataset_id = self.session.exec(dataset_id_stmt).first()

        if not dataset_id:
            raise DatasetNotFoundException(name=repo_name)

        delete_stmt = (
            sqlmodel_delete(Snapshot)
                .where(Snapshot.dataset_id == dataset_id)
                .where(Snapshot.snapshot_ts >= start_timestamp)
            )
        self.session.exec(delete_stmt)
        self.session.commit()


    def delete_snapshot_metrics_by_dataset_id(self, repo_name: str):
        dataset_id_stmt = select(Dataset.id).where(Dataset.repository_name == repo_name)
        dataset_id = self.session.exec(dataset_id_stmt).first()

        if not dataset_id:
            raise DatasetNotFoundException(name=repo_name)

        delete_stmt = sqlmodel_delete(Snapshot).where(Snapshot.dataset_id == dataset_id)
        self.session.exec(delete_stmt)
        self.session.commit()


    def update_class_statistics(self, dataset_id: UUID, repo_name: str, snapshot_ts: datetime, snapshot_ts_prev: Optional[datetime] = None):
        LOG.info(f"Repository name: {repo_name}: Updating 'snapshot' table with class metrics")
        LOG.info(f"Repository name: {repo_name}: Querying class metrics from GraphDB with ts_current={snapshot_ts} and ts_rev={snapshot_ts_prev}")
        
        # Retrieve metrics from GraphDB via SPARQL query in the csv format
        if snapshot_ts_prev:
            query = get_snapshot_classes_template(ts_current=snapshot_ts, ts_prev=snapshot_ts_prev)
        else:
            query = get_snapshot_classes_template(ts_current=snapshot_ts, ts_prev=snapshot_ts)
            snapshot_ts_prev = snapshot_ts  

        self.sparql_engine.setQuery(query)
        response = self.sparql_engine.query().convert() 

        # Parse CSV using pandas
        if isinstance(response, bytes):
            csv_text = response.decode('utf-8')
            df_metrics = pd.read_csv(StringIO(csv_text))
        else:
            raise ValueError("Unexpected response format from SPARQL query. Should be CSV bytes.")

        snapshots: list[Snapshot] = []
        for _, row in df_metrics.iterrows():
            snapshot = Snapshot(
                dataset_id=dataset_id,
                snapshot_ts=snapshot_ts,
                snapshot_ts_prev=snapshot_ts_prev,
                onto_class=row["onto_class"],
                onto_class_label=row["onto_class_label"] if pd.notna(row["onto_class_label"]) else None,
                parent_onto_class=row["parent_onto_class"] if pd.notna(row["parent_onto_class"]) else None,
                cnt_class_instances_current=row["cnt_class_instances_current"],
                cnt_class_instances_prev=row["cnt_class_instances_prev"],
                cnt_classes_added=row["cnt_classes_added"],
                cnt_classes_deleted=row["cnt_classes_deleted"]
            )
            snapshots.append(snapshot)
        
        if snapshots:
            LOG.info(f"Repository name: {repo_name}: Inserting {len(df_metrics)} computed class metrics into 'snapshot' table.")
            self.session.add_all(snapshots)
            self.session.commit()
            for snap in snapshots:
                self.session.refresh(snap) 
        else:
            LOG.warning(f"Repository name: {repo_name}: Query returned no metrics. Nothing will be inserted.")


    def update_property_statistics(self, dataset_id: UUID, repo_name: str, snapshot_ts: datetime, snapshot_ts_prev: datetime = None):
        LOG.info(f"Repository name: {repo_name}: Updating 'snapshot' table with property metrics")
        LOG.info(f"Repository name: {repo_name}: Querying property metrics from GraphDB with ts_current={snapshot_ts} and ts_rev={snapshot_ts_prev}")
        
        # Load property identifiers from app/persistance/graphdb/ontology_config.yml
        def load_property_identifiers(repo_name: str, config_path: str = "app/persistance/graphdb/ontology_config.yml") -> str:
            with open(config_path, "r", encoding="utf-8") as f:
                config = yaml.safe_load(f)

            identifiers = (
                config.get(repo_name, {}).get("propertyIdentifiers")
                or config["default"]["propertyIdentifiers"]
            )

            # return as comma-separated string
            return ", ".join(identifiers)

        property_identifiers = load_property_identifiers(repo_name)

        if snapshot_ts_prev:
            query = get_snapshot_properties_template(ts_current=snapshot_ts, ts_prev=snapshot_ts_prev, property_identifiers=property_identifiers)
        else:
            query = get_snapshot_properties_template(ts_current=snapshot_ts, ts_prev=snapshot_ts, property_identifiers=property_identifiers)

        # Retrieve metrics from GraphDB via SPARQL query in the csv format
        self.sparql_engine.setQuery(query)
        response = self.sparql_engine.query().convert() 

        # Parse CSV using pandas
        if isinstance(response, bytes):
            csv_text = response.decode('utf-8')
            df_metrics = pd.read_csv(StringIO(csv_text))
        else:
            raise ValueError("Unexpected response format from SPARQL query. Should be CSV bytes.")

        snapshots: list[Snapshot] = []
        for _, row in df_metrics.iterrows():
            snapshot = Snapshot(
                dataset_id=dataset_id,
                snapshot_ts=snapshot_ts,
                snapshot_ts_prev=snapshot_ts_prev,
                onto_property=row["onto_property"],
                onto_property_label=row["onto_property_label"] if pd.notna(row["onto_property_label"]) else None,
                parent_property=row["parent_property"] if pd.notna(row["parent_property"]) else None,
                cnt_property_instances_current=row["cnt_property_instances_current"],
                cnt_property_instances_prev=row["cnt_property_instances_prev"],
                cnt_properties_added=row["cnt_properties_added"],
                cnt_properties_deleted=row["cnt_properties_deleted"]
            )
            snapshots.append(snapshot)
        
        if snapshots:
            LOG.info(f"Repository name: {repo_name}: Inserting {len(df_metrics)} computed property metrics into 'snapshot' table.")
            self.session.add_all(snapshots)
            self.session.commit()
            for snap in snapshots:
                self.session.refresh(snap) 
        else:
            LOG.warning(f"Repository name: {repo_name}: Query returned no metrics. Nothing will be inserted.")


    def update_static_core_triples(self, dataset: Dataset, repo_name: str):
        # Update dataset table: static core triples 
        query = get_dataset_static_core_template()
        self.sparql_engine.setQuery(query)
        response = self.sparql_engine.query().convert() 
        if isinstance(response, bytes):
            csv_text = response.decode('utf-8')
            df_static_core = pd.read_csv(StringIO(csv_text))
        else:
            raise ValueError("Unexpected response format from SPARQL query. Should be CSV bytes.")
        value_int64 = df_static_core.at[0, "cnt_triples_static_core"]
        dataset.cnt_triples_static_core = int(value_int64) if pd.notna(value_int64) else -1
        
        LOG.info(
            f"Repository name: {repo_name}: "
            f"Updating static core triples to {dataset.cnt_triples_static_core} in 'dataset' table."
        )

        self.session.commit()
        self.session.refresh(dataset)

    def update_version_oblivious_triples(self, dataset: Dataset, repo_name: str):
        query = get_dataset_version_oblivious_template()
        self.sparql_engine.setQuery(query)
        response = self.sparql_engine.query().convert() 
        if isinstance(response, bytes):
            csv_text = response.decode('utf-8')
            df_vers_obl = pd.read_csv(StringIO(csv_text))
        else:
            raise ValueError("Unexpected response format from SPARQL query. Should be CSV bytes.")
        value_int64 = df_vers_obl.at[0, "cnt_triples_version_oblivious"]
        dataset.cnt_triples_version_oblivious = int(value_int64) if pd.notna(value_int64) else -1
        
        LOG.info(
            f"Repository name: {repo_name}: "
            f"Updating version oblivious triples to {dataset.cnt_triples_version_oblivious} in 'dataset' table."
        )

        self.session.commit()
        self.session.refresh(dataset)

