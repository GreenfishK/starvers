from flask import Blueprint, render_template, request, send_file, jsonify
import pandas as pd
import configparser
from datetime import datetime
import html
import logging
from io import StringIO
from app.gui.controller import GuiContr

routes = Blueprint('routes', __name__)
last_result_df = pd.DataFrame()

@routes.route("/", methods=["GET", "POST"])
def index():
    # Init
    global last_result_df
    result_html = None
    error_msg = None
    start_ts = None
    end_ts = None
    stats_plot = None

    logging.info("Load repository mappings from /code/app/gui/app/configs/RDF2Repo_mappings.ini")
    config = configparser.ConfigParser()
    config.read("/code/app/gui/configs/RDF2Repo_mappings.ini")
    repo_map = dict(config["repositories"])

    if request.method == "GET":
        selected_label = "orkg"
        repo = repo_map.get(selected_label)
        controller = GuiContr(repo_name=repo)

        # Get repo stats
        logging.info(f"Getting repository stats for {repo}")
        start_ts, end_ts, stats_plot = controller.get_repo_stats(repo)

        # Get tracking infos
        logging.info(f"Getting tracking infos for {repo}")
        rdf_dataset_url, polling_interval = controller.get_repo_tracking_infos(repo)

    if request.method == "POST":
        selected_label = request.form.get("repo")
        repo = repo_map.get(selected_label)
        timestamp_str = request.form.get("timestamp")
        query_text = request.form.get("sparql")

        try:
            # Initialize the controller
            controller = GuiContr(repo_name=repo)

            # Get repo stats
            start_ts, end_ts, stats_plot = controller.get_repo_stats(repo)

            logging.info(f"Submitting SPARQL query for execution against repository {repo} with timestamp {timestamp_str} .")
            timestamp = datetime.fromisoformat(timestamp_str) if timestamp_str else None
            df = controller.query(query_text, timestamp=timestamp, repo_name=repo)
            last_result_df = df

            logging.info("Processing DataFrame for HTML rendering")
            def iri_to_link(val):
                s = str(val)
                if s.startswith("<http://") or s.startswith("<https://"):
                    iri = s[1:-1]  # remove < and >
                    return f'<a href="{iri}" target="_blank">{html.escape(iri)}</a>'
                return html.escape(s)
            df = df.applymap(iri_to_link)

            logging.info("Embedding results into HTML")
            result_html = df.to_html(classes="table table-striped", index=False, escape=False)

        except Exception as e:
            error_msg = str(e)

    return render_template("index.html", 
                            result=result_html, error=error_msg, selected_repo=selected_label, 
                            repo_options=repo_map, stats_plot=stats_plot, ts_start=start_ts, ts_end=end_ts,
                            rdf_dataset_url=rdf_dataset_url, polling_interval=polling_interval)


@routes.route("/plot/<repo_label>")
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
        start, end, stats_plot = controller.get_repo_stats(repo_name)
        rdf_dataset_url, polling_interval = controller.get_repo_tracking_infos(repo_name)

        return jsonify({
            "plot_html": stats_plot,
            "rdf_dataset_url": rdf_dataset_url,
            "polling_interval": polling_interval
        })
    except Exception as e:
        logging.exception("Failed to generate plot and tracking info")
        return jsonify({"error": str(e)}), 500

@routes.route("/download")
def download():
    global last_result_df
    csv_buffer = StringIO()
    last_result_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    return send_file(csv_buffer, mimetype="text/csv", as_attachment=True, download_name="query_result.csv")