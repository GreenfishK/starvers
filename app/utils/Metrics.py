from io import StringIO
import pandas as pd

from app.Database import Session, engine
from app.models.DatasetModel import  Snapshot
from app.utils.graphdb.GraphDatabaseUtils import get_snapshot_metrics_template, create_engine
from app.LoggingConfig import get_logger

LOG = get_logger(__name__)

# Query the dataset_id, repository_name from all active datasets from the postgres 'dataset' table 
# and save into datasets variable with an empty list as values
datasets = {}

# Query all timestamps ('timestamp' column) from the csv file /mnt/data/evaluation/{repository_name}/{repsoitory_name}_timings.csv
# Re-arrange the timestamps into two columns, ts_current and ts_prev.
# ts_prev is ts_current, just displaced "downwards" by one row
# save the list of 2-tuples into the datasets dictionary

with Session(engine) as session:
    for dataset in datasets:
        # Setup connection to GraphDB for retrieving snapshot metrics
        sparql_engine = create_engine(dataset.repository_name)

        # Retrieve metrics from GraphDB via SPARQL query in the csv format
        LOG.info(f"Repository name: {dataset.repository_name}: Querying snapshot metrics from GraphDB")
        if latest_timestamp:
            query = get_snapshot_metrics_template(ts_current=version_timestamp, ts_prev=latest_timestamp)
        else:
            query = get_snapshot_metrics_template(ts_current=version_timestamp, ts_prev=version_timestamp)
        sparql_engine.setQuery(query)
        response = sparql_engine.query().convert() 

        # Parse CSV using pandas
        csv_text = response.decode('utf-8')
        df_metrics = pd.read_csv(StringIO(csv_text))

        LOG.info(f"Repository name: {dataset.repository_name}: Inserting {len(df_metrics)} computed metrics into 'snapshot' table: set all fields")

        snapshots = []
        for _, row in df_metrics.iterrows():
            snapshot = Snapshot(
                dataset_id=dataset.dataset_id,
                snapshot_ts=version_timestamp,
                snapshot_ts_prev=latest_timestamp if latest_timestamp else version_timestamp,
                onto_class=row["onto_class"],
                parent_onto_class=row["parent_onto_class"],
                cnt_class_instances_current=row["cnt_class_instances_current"],
                cnt_class_instances_prev=row["cnt_class_instances_prev"],
                cnt_classes_added=row["cnt_classes_added"],
                cnt_classes_deleted=row["cnt_classes_deleted"]
            )
            snapshots.append(snapshot)

        if snapshots:
            LOG.info(f"Repository name: {dataset.repository_name}: Inserting {len(snapshots)} snapshot(s) into 'snapshot' table.")
            session.add_all(snapshots)
            session.commit()
            for snap in snapshots:
                session.refresh(snap) 