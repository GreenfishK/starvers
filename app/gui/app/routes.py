from flask import Blueprint, render_template, request, send_file
import pandas as pd
from datetime import datetime
import html
import logging
from io import StringIO
from app.controller import GuiContr

routes = Blueprint('routes', __name__)
last_result_df = pd.DataFrame()

@routes.route("/", methods=["GET", "POST"])
def index():
    global last_result_df
    result_html = None
    error_msg = None
    repo = "orkg_v2"  # default value

    if request.method == "POST":
        repo = request.form.get("repo")
        timestamp_str = request.form.get("timestamp")
        query_text = request.form.get("sparql")

        try:
            # Initialize the controller
            controller = GuiContr(repo_name=repo)

            logging.info(f"Submitting SPARQL query for repository {repo} with timestamp {timestamp_str}")
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

    return render_template("index.html", result=result_html, error=error_msg, selected_repo=repo)

@routes.route("/download")
def download():
    global last_result_df
    csv_buffer = StringIO()
    last_result_df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    
    return send_file(csv_buffer, mimetype="text/csv", as_attachment=True, download_name="query_result.csv")