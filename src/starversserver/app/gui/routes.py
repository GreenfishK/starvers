from flask import Blueprint, render_template, request, send_file, jsonify, flash
import pandas as pd
import configparser
from datetime import datetime
import html
from io import BytesIO
import plotly.graph_objects as go
from plotly.io import to_html, to_json

from app.gui.controller import GuiContr
from app.enums.TimeAggregationEnum import TimeAggregation
from app.LoggingConfig import get_logger

logger = get_logger(__name__)

routes = Blueprint('routes', __name__)
last_result_df = pd.DataFrame()

@routes.route("/", methods=["GET"])
def index(): 
    # Init
    result_html = None

    logger.info("Load repository mappings from /code/app/gui/app/configs/RDF2Repo_mappings.ini")
    config = configparser.ConfigParser()
    config.read("/code/app/gui/configs/RDF2Repo_mappings.ini")
    repo_map = dict(config["repositories"])

    selected_label = "orkg"  # default selection
    repo_name: str = repo_map.get(selected_label)
    try:
        controller = GuiContr(repo_name=repo_name)
    except Exception as e:
        flash(f"Could not load dataset metadata: {e}", "danger")
        return render_template(
            "index.html",
            result=None,
            error=str(e),
            selected_repo=selected_label,
            repo_options=repo_map,
            evo_plot=None,
            ts_start=None,
            ts_end=None,
            ts_end_iso8601=None,
            rdf_dataset_url=None,
            polling_interval=None,
            next_run=None,
            cnt_triples_static_core=None,
            cnt_triples_version_oblivious=None
        )

    # Get repo stats
    logger.info(f"Getting repository stats for {repo_name}")
    ts_start, ts_end, fig_data, fig_layout = controller.build_timeseries()    
    ts_end_iso8601 = datetime.strptime(ts_end, "%d.%m.%Y %H:%M:%S.%f").isoformat()[:-3]
    logger.info(f"Timestamp: {ts_end}, {ts_end_iso8601}")

    evo_plot = to_html(
        go.Figure(
            data=fig_data,
            layout=fig_layout
        ),
        config={"scrollZoom": True, "responsive": True, "displayModeBar": False},
        full_html=False,
        include_plotlyjs=False,
        div_id="evo-plot"
    )

    # Get tracking infos
    dataset_infos = controller.get_dataset_infos()
    logger.info(f"Received dataset infos for {repo_name}: {dataset_infos}")

    return render_template(
        "index.html", 
        result=result_html,
        error=None,
        selected_repo=selected_label,
        repo_options=repo_map,
        evo_plot=evo_plot,
        ts_start=ts_start,
        ts_end=ts_end,
        ts_end_iso8601=ts_end_iso8601,
        rdf_dataset_url=dataset_infos[1],
        polling_interval=dataset_infos[2],
        next_run=dataset_infos[3].strftime("%Y-%m-%d %H:%M:%S") if dataset_infos[3] else None,
        cnt_triples_static_core=dataset_infos[4],
        cnt_triples_version_oblivious=dataset_infos[5]
    )


@routes.route("/infos/<repo_label>")
def get_repo_infos(repo_label):
    logger.info(f"Received request for updated plot and tracking info of repo: {repo_label}")
    config = configparser.ConfigParser()
    config.read("/code/app/gui/configs/RDF2Repo_mappings.ini")
    repo_map = dict(config["repositories"])
    repo_name = repo_map.get(repo_label)

    if not repo_name:
        logger.error(f"Repository label '{repo_label}' not found in config.")
        return jsonify({"error": "Repository not found"}), 404

    aggregation = request.args.get("agg", "DAY")  
    try:
        time_aggr = TimeAggregation[aggregation.upper()]
        time_aggr_map = {
            TimeAggregation.HOUR: 0,
            TimeAggregation.DAY: 1,
            TimeAggregation.WEEK: 2
        }
        active_time_aggr = time_aggr_map[time_aggr]
    except KeyError:
        return jsonify({"error": "Invalid aggregation level"}), 400
    
    try:
        controller = GuiContr(repo_name=repo_name)
        _, _, fig_data, fig_layout = controller.build_timeseries(time_aggr, active_time_aggr)
        dataset_infos = controller.get_dataset_infos()
        logger.info(f"Received dataset infos for {repo_name}: {dataset_infos}")
        evo_plot = go.Figure(data=fig_data, layout=fig_layout)

        return jsonify({
            "evo_plot": to_json(evo_plot),
            "rdf_dataset_url": dataset_infos[1],
            "polling_interval": dataset_infos[2],
            "cnt_triples_static_core": dataset_infos[4],
            "cnt_triples_version_oblivious": dataset_infos[5],
            "next_run": dataset_infos[3].strftime("%Y-%m-%d %H:%M:%S") if dataset_infos[3] else None,
        })
    except Exception as e:
        logger.exception("Failed to generate plot and tracking info")
        return jsonify({"error": str(e)}), 500


@routes.route("/statistics", methods=["GET"])
def get_onto_hierarchy():
    # Repo name
    config = configparser.ConfigParser()
    config.read("/code/app/gui/configs/RDF2Repo_mappings.ini")
    repo_map = dict(config["repositories"])

    try:
        selected_label = request.args.get("repo")
        repo_name = repo_map.get(selected_label)
        logger.info(f"Received selected repository name from frontend: {repo_name}")

        # snapshot_ts
        snapshot_ts = request.args.get("timestamp")
        logger.info(f"Received timestamp from selected data point from frontend: {snapshot_ts}")
    except Exception as e:
        logger.error(f"Repo or snapshot timestamp not received from frontend. Repo name: {repo_name}; Snapshot timestamp: {snapshot_ts}")

    try:
        controller = GuiContr(repo_name=repo_name)
        class_hierarchy, property_hierarchy, snapshot_ts_actual = controller.get_snapshot_stats(snapshot_ts)
        
        return jsonify({
            "class_hierarchy": class_hierarchy,
            "property_hierarchy": property_hierarchy,
            "snapshot_ts": snapshot_ts_actual.isoformat() if snapshot_ts_actual else None
        })

    except Exception as e:
        logger.exception(f"Failed to retrieve snapshot statistics from database: {str(e)}")
        return jsonify({"error": str(e)}), 500


@routes.route("/query", methods=["POST"])
def run_query():
    config = configparser.ConfigParser()
    config.read("/code/app/gui/configs/RDF2Repo_mappings.ini")
    repo_map = dict(config["repositories"])
    selected_label = request.form.get("repo")
    
    repo = repo_map.get(selected_label)
    timestamp_str = request.form.get("timestamp")
    query_text = request.form.get("sparql")

    try:
        controller = GuiContr(repo_name=repo)
        timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else None
        df, timesamped_query = controller.query(query_text, timestamp=timestamp)

        global last_result_df
        last_result_df = df if not df.empty else pd.DataFrame()

        # Convert IRI to link
        def iri_to_link(val):
            s = str(val)
            if s.startswith("<http://") or s.startswith("<https://"):
                iri = s[1:-1]
                return f'<a href="{iri}" target="_blank">{html.escape(iri)}</a>'
            return html.escape(s)
        
        df = df.applymap(iri_to_link)
        result_set = df.to_html(classes="table table-striped", index=False, escape=False)

        return jsonify({"result_set": result_set, "timestamped_query": timesamped_query})
    
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@routes.route("/download")
def download():
    global last_result_df

    # Write CSV to string first
    csv_string = last_result_df.to_csv(index=False)

    # Encode the string into bytes and wrap it with BytesIO
    csv_buffer = BytesIO(csv_string.encode("utf-8"))
    csv_buffer.seek(0)

    return send_file(
        csv_buffer,
        mimetype="text/csv",
        as_attachment=True,
        download_name="query_result.csv"
    )