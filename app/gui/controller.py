from datetime import datetime, timedelta
import pandas as pd
import logging
import plotly.graph_objects as go

# starvers and starversServer imports
from starvers.starvers import TripleStoreEngine
from app.services.ManagementService import get_dataset_metadata_by_repo_name
from app.Database import get_session
from app.enums.TimeAggregationEnum import TimeAggregation



class GuiContr:
    def __init__(self, repo_name: str = "orkg_v2"):
        self.repo_name = repo_name
        self.__graph_db_get_endpoint = f"http://rdfstore:7200/repositories/{repo_name}" 
        self.__graph_db_post_endpoint = f"http://rdfstore:7200/repositories/{repo_name}/statements" 
        self.__starvers_engine = TripleStoreEngine(self.__graph_db_get_endpoint, self.__graph_db_post_endpoint, skip_connection_test=True)


    def query(self, query: str, timestamp: datetime = None, query_as_timestamped: bool = True) -> pd.DataFrame:
        if timestamp is not None and query_as_timestamped:
            logging.info(f"Execute timestamped query with timestamp={timestamp}")
        else:
            logging.info("Execute query without timestamp")
        
        result_set_df = self.__starvers_engine.query(query, timestamp, query_as_timestamped)
        timestamped_query = self.__starvers_engine.timestamped_query
        timestamped_query = timestamped_query.lstrip()
        return result_set_df, timestamped_query


    def get_repo_stats(self, time_aggr: TimeAggregation = TimeAggregation.DAY, active_time_aggr: int = 1):
        repo_name = self.repo_name
        path = f"/code/evaluation/{repo_name}/{repo_name}_timings.csv"
        df = pd.read_csv(path)
        df.columns = df.columns.str.strip()

        # Parse timestamp
        df["timestamp"] = df["timestamp"].apply(lambda ts: datetime.strptime(ts[:15], "%Y%m%d-%H%M%S"))

        # Aggregate by given time interval
        df = df.set_index("timestamp")
        logging.info(f"Aggregating data by {time_aggr.value} intervals")
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

        timestamps = agg["timestamp"].dt.strftime("%d.%m.%Y\n%H:%M:%S")
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
                hovertemplates_ins.append(f"{ins:,} insertions (net)")

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
                    ins_base.append(base_y - dels)
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
            marker_color="green",
            name="Insertions",
            width=widths_ins,
            hovertemplate=hovertemplates_ins
        ))

        fig.add_trace(go.Bar(
            x=timestamps,
            y=del_y,
            base=del_base,
            marker_color="red",
            name="Deletions",
            width=widths_del,
            hovertemplate=hovertemplates_del
        ))

        fig.add_trace(go.Scatter(
            x=timestamps,
            y=[0] + total.tolist(),
            mode="lines+markers",
            name='Total Triples',
            line=dict(color="blue", width=1),  # thin blue line
        ))

        fig.update_layout(
            xaxis_title="Time",
            yaxis_title="Triple Count",
            dragmode="pan",
            height=500,
            barmode='overlay',
            plot_bgcolor='white',  
            paper_bgcolor='white',  
            xaxis=dict(
                showgrid=True,
                gridcolor='lightgray', 
                gridwidth=1,
                rangeslider=dict(visible=False),
                fixedrange=False
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

        # Add dropdown for time aggregation
        fig.update_layout(
            updatemenus=[
                dict(
                    type="buttons",
                    direction="left",
                    showactive=True,
                    active=active_time_aggr,  
                    buttons=[
                        dict(label="Hour", method="restyle", args=[[], []], args2=["HOUR"]),
                        dict(label="Day", method="restyle", args=[[], []], args2=["DAY"]),
                        dict(label="Week", method="restyle", args=[[], []], args2=["WEEK"]),
                    ],
                    x=0,
                    xanchor="left",
                    y=1.15,
                    yanchor="top"
                )
            ],
        )

        start = agg["timestamp"].min().strftime("%d.%m.%Y %H:%M:%S")
        end = agg["timestamp"].max().strftime("%d.%m.%Y %H:%M:%S")

        return start, end, fig.data, fig.layout


    def get_repo_tracking_infos(self):
        repo_name = self.repo_name

        session = next(get_session())
        tracking_infos = get_dataset_metadata_by_repo_name(repo_name, session)
        rdf_dataset_url = tracking_infos[1]
        formatted_polling_interval = _format_polling_interval(tracking_infos[2])
        next_run = tracking_infos[3]
        session.close()

        logging.info(f"Tracking infos for {repo_name}: {rdf_dataset_url}; {formatted_polling_interval}; {next_run}")
        return rdf_dataset_url, formatted_polling_interval, next_run
    

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
