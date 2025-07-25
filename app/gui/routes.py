from flask import Blueprint, render_template, request, send_file, jsonify
import pandas as pd
import configparser
from datetime import datetime
import html
import logging
from io import BytesIO
import plotly.graph_objects as go
from plotly.io import to_html, to_json

from app.gui.controller import GuiContr
from app.enums.TimeAggregationEnum import TimeAggregation


routes = Blueprint('routes', __name__)
last_result_df = pd.DataFrame()

@routes.route("/", methods=["GET"])
def index():
    # Init
    result_html = None
    start_ts = None
    end_ts = None
    stats_plot = None
    rdf_dataset_url = None
    polling_interval = None

    logging.info("Load repository mappings from /code/app/gui/app/configs/RDF2Repo_mappings.ini")
    config = configparser.ConfigParser()
    config.read("/code/app/gui/configs/RDF2Repo_mappings.ini")
    repo_map = dict(config["repositories"])

    selected_label = "orkg"  # default selection
    repo = repo_map.get(selected_label)
    controller = GuiContr(repo_name=repo)

    # Get repo stats
    logging.info(f"Getting repository stats for {repo}")
    start_ts, end_ts, fig_data, fig_layout = controller.get_repo_stats()

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
    logging.info(f"Getting tracking infos for {repo}")
    rdf_dataset_url, polling_interval, next_run = controller.get_repo_tracking_infos()

    return render_template(
        "index.html", 
        result=result_html,
        error=None,
        selected_repo=selected_label,
        repo_options=repo_map,
        evo_plot=evo_plot,
        ts_start=start_ts,
        ts_end=end_ts,
        rdf_dataset_url=rdf_dataset_url,
        polling_interval=polling_interval,
        next_run=next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else None
    )


@routes.route("/infos/<repo_label>")
def get_repo_infos(repo_label):
    logging.info(f"Received request for updated plot and tracking info of repo: {repo_label}")
    config = configparser.ConfigParser()
    config.read("/code/app/gui/configs/RDF2Repo_mappings.ini")
    repo_map = dict(config["repositories"])
    repo_name = repo_map.get(repo_label)

    if not repo_name:
        logging.error(f"Repository label '{repo_label}' not found in config.")
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
        start, end, fig_data, fig_layout = controller.get_repo_stats(time_aggr, active_time_aggr)
        rdf_dataset_url, polling_interval, next_run = controller.get_repo_tracking_infos()

        evo_plot = go.Figure(data=fig_data, layout=fig_layout)

        return jsonify({
            "evo_plot": to_json(evo_plot),
            "rdf_dataset_url": rdf_dataset_url,
            "polling_interval": polling_interval,
            "next_run": next_run.strftime("%Y-%m-%d %H:%M:%S") if next_run else None,
        })
    except Exception as e:
        logging.exception("Failed to generate plot and tracking info")
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