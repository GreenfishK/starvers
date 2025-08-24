from datetime import datetime, timedelta
import pandas as pd
import plotly.graph_objects as go
from collections import defaultdict
import networkx as nx
from datetime import datetime

# starvers and starversServer imports
from starvers.starvers import TripleStoreEngine
from app.services.ManagementService import get_dataset_metadata_by_repo_name, get_snapshot_stats_by_repo_name_and_snapshot_ts
from app.Database import get_session
from app.enums.TimeAggregationEnum import TimeAggregation
from app.AppConfig import Settings
from app.LoggingConfig import get_logger

logger = get_logger(__name__)

class GuiContr:
    def __init__(self, repo_name: str = "orkg_v2"):
        self.repo_name = repo_name
        self.__graph_db_get_endpoint = Settings().graph_db_url_get_endpoint.replace('{:repo_name}', repo_name)
        self.__graph_db_post_endpoint = Settings().graph_db_url_post_endpoint.replace('{:repo_name}', repo_name)
        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint, skip_connection_test=True)
        try:
            session = next(get_session())
            self.dataset_infos = get_dataset_metadata_by_repo_name(repo_name, session)
        except (Exception, RuntimeError) as e:
            logger.error(f"Values could not be retrieved from the Postgres database. Original message: {str(e)}")
            raise RuntimeError(f"Failed to fetch dataset metadata: {e}")
        
        session.close()
        logger.info(f"GET endpoint: {self.__graph_db_get_endpoint}\nPOST endpoint: {self.__graph_db_post_endpoint}")


    def query(self, query: str, timestamp: datetime = None, query_as_timestamped: bool = True) -> pd.DataFrame:
        if timestamp is not None and query_as_timestamped:
            logger.info(f"Execute timestamped query with timestamp={timestamp}")
        else:
            logger.info("Execute query without timestamp")
        
        result_set_df = pd.DataFrame()
        timestamped_query = ""
        try:
            result_set_df = self.__starvers_engine.query(query, timestamp, query_as_timestamped)
            timestamped_query = self.__starvers_engine.timestamped_query
            timestamped_query = timestamped_query.lstrip()
        except Exception as e:
            logger.error(f"Error executing query: {e}")

        return result_set_df, timestamped_query


    def build_timeseries(self, time_aggr: TimeAggregation = TimeAggregation.DAY, active_time_aggr: int = 1):
        repo_name = self.repo_name
        path = f"/data/evaluation/{repo_name}/{repo_name}_timings.csv"
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()

        # Parse timestamp
        df["timestamp"] = df["timestamp"].apply(lambda ts: datetime.strptime(ts[:19], "%Y%m%d-%H%M%S_%f"))
        ts_start = df["timestamp"].min().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
        ts_end = df["timestamp"].max().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]

        # Aggregate by given time interval
        df = df.set_index("timestamp")
        logger.info(f"Aggregating data by {time_aggr.value} intervals")
        if time_aggr.name == "WEEK":
            agg = df.resample(time_aggr.value, label='right', closed='right').agg({
                "insertions": "sum",
                "deletions": "sum",
                "cnt_triples": "last"
            })
        else:
            agg = df.resample(time_aggr.value).agg({
                "insertions": "sum",
                "deletions": "sum",
                "cnt_triples": "last"
            })

        # Fill missing values
        agg["insertions"] = agg["insertions"].fillna(0).astype(int)
        agg["deletions"] = agg["deletions"].fillna(0).astype(int)
        agg["cnt_triples"] = agg["cnt_triples"].fillna(method="ffill").astype(int)

        agg = agg.reset_index()

        timeformat = "%d.%m.%Y\n%H:%M:%S" if time_aggr.name == "HOUR" else "%d.%m.%Y"
        timestamps = agg["timestamp"].dt.strftime(timeformat) 
        total = agg["cnt_triples"].astype(int)
        insertions = agg["insertions"].astype(int)
        deletions = agg["deletions"].astype(int)

        # Prepare arrays for traces
        ins_y = []
        ins_base = []
        del_y = []
        del_base = []
        widths_ins = []
        widths_del = []
        hovertemplates_ins = []
        hovertemplates_del = []

        for i in range(len(agg)):
            base_y = total.iloc[i - 1] if i > 0 else 0
            ins = insertions.iloc[i]
            dels = deletions.iloc[i]
            net = ins - dels

            if net > 0:
                ins_y.append(net)
                ins_base.append(base_y)
                widths_ins.append(0.4)
                widths_del.append(0.1)
                hovertemplates_ins.append(f"{net:,} insertions (net)")

                if dels > 0:
                    del_y.append(dels)
                    del_base.append(base_y + net)
                    hovertemplates_del.append(f"{dels:,} deletions")
                else:
                    del_y.append(0)
                    del_base.append(0)
                    hovertemplates_del.append("No deletions")
            else:
                del_y.append(net)
                del_base.append(base_y)
                widths_ins.append(0.1)
                widths_del.append(0.4)
                hovertemplates_del.append(f"{-net:,} deletions (net)")

                if ins > 0:
                    ins_y.append(ins)
                    ins_base.append(base_y + net)
                    hovertemplates_ins.append(f"{ins:,} insertions")
                else:
                    ins_y.append(0)
                    ins_base.append(0)
                    hovertemplates_ins.append("No insertions")

        fig = go.Figure()

        fig.add_trace(go.Bar(
            x=timestamps,
            y=ins_y,
            base=ins_base,
            marker_color="#007E71",
            name="Insertions",
            width=widths_ins,
            hovertemplate=hovertemplates_ins
        ))

        fig.add_trace(go.Bar(
            x=timestamps,
            y=del_y,
            base=del_base,
            marker_color="#BA4682",
            name="Deletions",
            width=widths_del,
            hovertemplate=hovertemplates_del
        ))

        fig.add_trace(go.Scatter(
            x=timestamps,
            y=total.tolist(),
            mode="lines+markers",
            name='Total Triples',
            line=dict(color="#5485AB", width=1)
        ))

        # Multi-line hierarchical tick labels

        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Triple Count",
            dragmode="pan",
            height=500,
            barmode='overlay',
            plot_bgcolor='white',  
            paper_bgcolor='white',  
            margin=dict(t=30, b=40, l=50, r=20), 
            showlegend=False,
            xaxis=dict(
                showgrid=True,
                gridcolor='lightgray', 
                gridwidth=1,
                rangeslider=dict(visible=False),
                fixedrange=False,

            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='lightgray',
                gridwidth=1,
                autorange=True,
                fixedrange=True,  # prevent manual manual y panning, but allow zoom
                tickformat=","
            )
        )

        return ts_start, ts_end, fig.data, fig.layout


    def get_dataset_infos(self):
        logger.info(f"Returning dataset infos for repo: {self.repo_name}")

        return self.dataset_infos
    

    def get_onto_hierarchy(self, snapshot_ts):
        logger.info(f"Retrieving snapshot statistics for timestamp: {snapshot_ts}")
        
        repo_name = self.repo_name
        session = next(get_session())
        snapshot_ts = datetime.fromisoformat(snapshot_ts)
        raw_df = get_snapshot_stats_by_repo_name_and_snapshot_ts(repo_name, snapshot_ts, session)
        session.close()

        if raw_df.empty:
            return [], None  # empty tree and no timestamp

        snapshot_ts_actual = raw_df["snapshot_ts"].iloc[0]
        raw_df = raw_df.sort_values(by="cnt_class_instances_current", ascending=False)

        # Ensure that every parent class is present in the dataframe
        # This is necessary is we discovered that some ontologies use the subClassOf property
        # without the object node being a class itself.
        all_onto_classes = set(raw_df["onto_class"].tolist())
        all_parents = set(raw_df["parent_onto_class"].dropna().tolist())

        missing_parents = all_parents - all_onto_classes
        if missing_parents:
            logger.info(f"Adding {len(missing_parents)} missing parent classes to dataframe.")
            for mp in missing_parents:
                raw_df = pd.concat([
                    raw_df,
                    pd.DataFrame([{
                        "onto_class": mp,
                        "parent_onto_class": None,
                        "cnt_class_instances_current": 0,
                        "cnt_class_instances_prev": 0,
                        "cnt_classes_added": 0,
                        "cnt_classes_deleted": 0,
                        "snapshot_ts": snapshot_ts_actual
                    }])
                ], ignore_index=True)
        
        G = nx.DiGraph()
        class_data = {}

        # Add a synthetic root node
        ROOT = "Thing"
        G.add_node(ROOT, synthetic=True)

        for _, row in raw_df.iterrows():
            parent = row["parent_onto_class"] or ROOT
            child = row["onto_class"]

            # Add nodes with metadata
            class_data[child] = {
                "cnt_class_instances_current": row["cnt_class_instances_current"],
                "cnt_class_instances_prev": row["cnt_class_instances_prev"],
                "cnt_classes_added": row["cnt_classes_added"],
                "cnt_classes_deleted": row["cnt_classes_deleted"],
            }

            G.add_node(child)
            G.add_edge(parent, child)

        def aggregate_stats(node):
            # Start with this node's own counts (0 if not in class_data)
            own_stats = {
                "cnt_class_instances_current": class_data.get(node, {}).get("cnt_class_instances_current", 0),
                "cnt_class_instances_prev": class_data.get(node, {}).get("cnt_class_instances_prev", 0),
                "cnt_classes_added": class_data.get(node, {}).get("cnt_classes_added", 0),
                "cnt_classes_deleted": class_data.get(node, {}).get("cnt_classes_deleted", 0),
            }

            # Add stats from all children
            for child in G.successors(node):
                child_stats = aggregate_stats(child)
                for k in own_stats:
                    own_stats[k] += child_stats[k]

            return own_stats
        
        # Build the tree from a node recursively
        def build_tree(node):
            stats = aggregate_stats(node)
            return {
                "id": node,
                "cnt_class_instances": stats["cnt_class_instances_current"],
                "cnt_class_instances_prev": stats["cnt_class_instances_prev"],
                "cnt_classes_added": stats["cnt_classes_added"],
                "cnt_classes_deleted": stats["cnt_classes_deleted"],
                "children": [build_tree(child) for child in G.successors(node)]
            }

        hierarchy = [build_tree(child) for child in G.successors(ROOT)]

        return hierarchy, snapshot_ts_actual


def _format_polling_interval(seconds: int) -> str:
    seconds = int(seconds)
    if seconds < 60:
        return f"{seconds} seconds"
    elif seconds < 3600:
        minutes = seconds // 60
        sec = seconds % 60
        return f"{minutes} minutes {sec} seconds"
    elif seconds < 86400:
        hours = seconds // 3600
        remainder = seconds % 3600
        minutes = remainder // 60
        sec = remainder % 60
        return f"{hours:02d}:{minutes:02d}:{sec:02d}"
    else:
        days = seconds // 86400
        remainder = seconds % 86400
        hours = remainder // 3600
        remainder %= 3600
        minutes = remainder // 60
        sec = remainder % 60
        return f"{days} days, {hours:02d}:{minutes:02d}:{sec:02d}"


