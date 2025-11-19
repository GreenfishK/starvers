from datetime import datetime
from logging import Logger
import pandas as pd
import plotly.graph_objects as go
import networkx as nx
from typing import List, Dict, Any, Optional, Tuple

# starvers and starversServer imports
from app.utils.starvers.starvers import TripleStoreEngine
from app.services.ManagementService import get_dataset_metadata_by_repo_name, get_snapshot_stats_by_repo_name_and_snapshot_ts
from app.persistance.Database import get_session
from app.enums.TimeAggregationEnum import TimeAggregation
from app.AppConfig import Settings
from app.LoggingConfig import get_logger

logger: Logger = get_logger(__name__)

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
            logger.info(f"Result set contains {len(result_set_df)} records.")
            timestamped_query = self.__starvers_engine.timestamped_query
            timestamped_query = timestamped_query.lstrip()
        except Exception as e:
            raise Exception(f"Error executing query: {e}")

        return result_set_df, timestamped_query


    def build_timeseries(self, time_aggr: TimeAggregation = TimeAggregation.DAY, active_time_aggr: int = 1) -> tuple[str, str, Any, go.Layout]:
        repo_name = self.repo_name
        path = f"/data/evaluation/{repo_name}/{repo_name}_timings.csv"
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()

        # Parse timestamp
        df["timestamp"] = df["timestamp"].apply(lambda ts: datetime.strptime(ts[:19], "%Y%m%d-%H%M%S_%f"))
        ts_start:str = df["timestamp"].min().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]
        ts_end:str = df["timestamp"].max().strftime("%d.%m.%Y %H:%M:%S.%f")[:-3]

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
        hovertemplates_total = []

        # compute time differences in milliseconds
        diffs_ms = agg["timestamp"].diff().dt.total_seconds().fillna(0) * 1000
        valid_diffs = diffs_ms[1:]  # skip the first NaN
        if len(valid_diffs) > 0 and valid_diffs.median() > 0:
            median_delta_ms = int(valid_diffs.median())
        else:
            # fallback to 1 day if single point or weird data
            median_delta_ms = 24 * 3600 * 1000

        # choose bar width as a fraction of the median spacing (0.6 is a good start)
        bar_width_ms = max(1, int(median_delta_ms * 0.6))
        widths_ins = [bar_width_ms] * len(agg)
        widths_del = [bar_width_ms] * len(agg)

        for i in range(len(agg)):
            base_y = total.iloc[i - 1] if i > 0 else 0
            ins = insertions.iloc[i]
            dels = deletions.iloc[i]
            net = ins - dels

            logger.info(f"Insertions: {ins}; Deletions: {dels}; Net: {net}")

            if net > 0:
                ins_y.append(net)
                ins_base.append(base_y)
                hovertemplates_ins.append(f"{net:,} insertions (net)")

                if dels > 0:
                    del_y.append(dels)
                    del_base.append(base_y + net)
                    hovertemplates_del.append(f"{dels:,} deletions")
                else:
                    del_y.append(0)
                    del_base.append(0)
                    hovertemplates_del.append("No deletions")
            elif net < 0:
                del_y.append(net)
                del_base.append(base_y)
                hovertemplates_del.append(f"{-net:,} deletions (net)")

                if ins > 0:
                    ins_y.append(ins)
                    ins_base.append(base_y - dels)
                    hovertemplates_ins.append(f"{ins:,} insertions")
                else:
                    ins_y.append(0)
                    ins_base.append(0)
                    hovertemplates_ins.append("No insertions")
            else:
                ins_y.append(0)
                del_y.append(0)
                ins_base.append(0)
                del_base.append(0)
                hovertemplates_ins.append(f"dummy")
                hovertemplates_del.append(f"dummy")

            hovertemplates_total.append(f"Insertions: {ins:,}\nDeletions: {dels:,}\nNet: {net:,}")


        fig = go.Figure()

        # compute timestamps and a sensible bar width in ms 
        timestamps = [dt.isoformat() for dt in agg["timestamp"].dt.to_pydatetime()]

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
            line=dict(color="#5485AB", width=1),
            hovertemplate=hovertemplates_total
        ))

        # compute x_range using datetime, not string
        margin_width = pd.Timedelta(milliseconds=bar_width_ms / 1.5)

        if len(agg) > 14:
            x_start_dt = agg["timestamp"].iloc[-14] - margin_width
            x_end_dt = agg["timestamp"].iloc[-1] + margin_width
        else:
            x_start_dt = agg["timestamp"].iloc[0] - margin_width
            x_end_dt = agg["timestamp"].iloc[-1] + margin_width

        # convert to ISO strings for Plotly
        x_range = [x_start_dt.isoformat(), x_end_dt.isoformat()]

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
                range=x_range
            ),
            yaxis=dict(
                showgrid=True,
                gridcolor='lightgray',
                gridwidth=1,
                autorange=True,
                fixedrange=True,  
                tickformat=","
            )
        )

        return ts_start, ts_end, fig.data, fig.layout


    def get_dataset_infos(self):
        logger.info(f"Returning dataset infos for repo: {self.repo_name}")

        return self.dataset_infos
    

    def get_snapshot_stats(self, snapshot_ts: str) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], Optional[datetime]]:
        logger.info(f"Retrieving snapshot statistics for timestamp: {snapshot_ts}")
        
        repo_name: str = self.repo_name
        session = next(get_session())
        snapshot_ts_dt: datetime = datetime.fromisoformat(snapshot_ts)
        raw_df: pd.DataFrame = get_snapshot_stats_by_repo_name_and_snapshot_ts(repo_name, snapshot_ts_dt, session)
        session.close()

        if raw_df.empty:
            return [], [], None  
                
        snapshot_ts_actual = raw_df["snapshot_ts"].iloc[0]

        unified_columns = [
            "id", "label", "parent", 
            "cnt_instances_current", "cnt_instances_prev", "cnt_added", "cnt_deleted",
        ]

        raw_df_class = raw_df[[
            "onto_class", "onto_class_label", "parent_onto_class",
            "cnt_class_instances_current", "cnt_class_instances_prev",
            "cnt_classes_added", "cnt_classes_deleted",
        ]].dropna(how="all")
        raw_df_class.columns = unified_columns
        raw_df_class = raw_df_class.sort_values(by="cnt_instances_current", ascending=False)


        raw_df_property = raw_df[[
            "onto_property", "onto_property_label", "parent_property",
            "cnt_property_instances_current", "cnt_property_instances_prev",
            "cnt_properties_added", "cnt_properties_deleted",
        ]].dropna(how="all")
        raw_df_property.columns = unified_columns
        raw_df_property = raw_df_property.sort_values(by="cnt_instances_current", ascending=False)


        def build_hierarchy(raw_df: pd.DataFrame, id_col: str, label_col: str, parent_col: str, count_cols: List[str]) -> List[Dict[str, Any]]:
            """
            Build a hierarchy (tree) for classes or properties.
            """

            # Ensure missing parents are inserted
            all_nodes: set[str] = set(raw_df[id_col].dropna().tolist())
            all_parents: set[str] = set(raw_df[parent_col].dropna().tolist())
            missing_parents: set[str] = all_parents - all_nodes

            if missing_parents:
                logger.info(f"Adding {len(missing_parents)} missing parent nodes to dataframe for {id_col}.")
                for mp in missing_parents:
                    raw_df = pd.concat([
                        raw_df,
                        pd.DataFrame([{
                            id_col: mp,
                            label_col: mp, 
                            parent_col: None,
                            count_cols[0]: 0,
                            count_cols[1]: 0,
                            count_cols[2]: 0,
                            count_cols[3]: 0,
                            "snapshot_ts": snapshot_ts_actual  
                        }])
                    ], ignore_index=True)

            # Build graph
            G: nx.DiGraph = nx.DiGraph()
            node_data: Dict[str, Dict[str, Any]] = {}

            ROOT: str = f"ROOT_{id_col}"
            G.add_node(ROOT, synthetic=True)

            for _, row in raw_df.iterrows():
                parent: str = row[parent_col] or ROOT
                child: str = row[id_col]

                # Save stats & label
                node_data[child] = {
                    "label": row.get(label_col, child),
                    count_cols[0]: int(row[count_cols[0]]),
                    count_cols[1]: int(row[count_cols[1]]),
                    count_cols[2]: int(row[count_cols[2]]),
                    count_cols[3]: int(row[count_cols[3]]),
                }

                G.add_node(child)
                G.add_edge(parent, child)

            def aggregate_stats(node: str) -> Dict[str, int]:
                own_stats: Dict[str, int] = {k: node_data.get(node, {}).get(k, 0) for k in count_cols}
                for child in G.successors(node):
                    child_stats: Dict[str, int] = aggregate_stats(child)
                    for k in count_cols:
                        own_stats[k] += child_stats[k]
                return own_stats

            def build_tree(node: str) -> Dict[str, Any]:
                stats: Dict[str, int] = aggregate_stats(node)
                return {
                    "id": node,
                    "label": node_data.get(node, {}).get("label", node) or node,
                    count_cols[0]: stats[count_cols[0]],
                    count_cols[1]: stats[count_cols[1]],
                    count_cols[2]: stats[count_cols[2]],
                    count_cols[3]: stats[count_cols[3]],
                    "children": [build_tree(child) for child in G.successors(node)]
                }

            return [build_tree(child) for child in G.successors(ROOT)]

        # Build class hierarchy
        class_hierarchy = build_hierarchy(
            raw_df_class,
            id_col="id",
            label_col="label",
            parent_col="parent",
            count_cols=[
                "cnt_instances_current",
                "cnt_instances_prev",
                "cnt_added",
                "cnt_deleted"
            ]
        )

        # Build property hierarchy
        property_hierarchy = build_hierarchy(
            raw_df_property,
            id_col="id",
            label_col="label",
            parent_col="parent",
            count_cols=[
                "cnt_instances_current",
                "cnt_instances_prev",
                "cnt_added",
                "cnt_deleted"
            ]
        )

        return class_hierarchy, property_hierarchy, snapshot_ts_actual


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


