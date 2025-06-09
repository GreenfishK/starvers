from flask import Blueprint, render_template, request, send_file, jsonify
import pandas as pd
import configparser
from datetime import datetime
import html
import logging
from io import BytesIO
from app.gui.controller import GuiContr

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
    start_ts, end_ts, delta_plot, total_plot = controller.get_repo_stats()

    # Get tracking infos
    logging.info(f"Getting tracking infos for {repo}")
    rdf_dataset_url, polling_interval = controller.get_repo_tracking_infos()

    return render_template(
        "index.html", 
        result=result_html,
        error=None,
        selected_repo=selected_label,
        repo_options=repo_map,
        delta_plot=delta_plot,
        total_plot=total_plot,
        ts_start=start_ts,
        ts_end=end_ts,
        rdf_dataset_url=rdf_dataset_url,
        polling_interval=polling_interval
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

    try:
        controller = GuiContr(repo_name=repo_name)
        start, end, delta_plot, total_plot = controller.get_repo_stats()
        rdf_dataset_url, polling_interval = controller.get_repo_tracking_infos()

        return jsonify({
            "delta_plot_html": delta_plot,
            "total_plot_html": total_plot,
            "rdf_dataset_url": rdf_dataset_url,
            "polling_interval": polling_interval
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
        html_table = df.to_html(classes="table table-striped", index=False, escape=False)
        return jsonify({"html": html_table, "timestamped_query": timesamped_query})
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