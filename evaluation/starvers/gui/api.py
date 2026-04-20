"""
api.py – Lightweight Flask backend for the Starvers Evaluation GUI.

Serves:
  GET /evaluation/starvers/api/runs         -> list of runs (newest first)
  GET /evaluation/starvers/api/runs/<ts>    -> detail for one run
  GET /evaluation/starvers/                 -> index.html GUI

Run (inside container):
  python api.py

Environment variables:
  DATA_DIR   – host path where run directories live (default: /starvers_eval/data)
  PORT       – port to bind (default: 8080)
"""

import csv
import os
from pathlib import Path
from datetime import datetime
from flask import Flask, jsonify, send_from_directory, abort

app = Flask(__name__, static_folder=".")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/starvers_eval/data"))
PORT = int(os.environ.get("PORT", 8080))

ALL_STEPS = [
    "download", "preprocess_data", "construct_datasets", "ingest",
    "construct_queries", "evaluate", "verify_results", "visualize",
]


def _read_run(run_dir: Path) -> dict:
    csv_path = run_dir / "execution.csv"
    steps = []
    if csv_path.exists():
        with open(csv_path, newline="") as f:
            steps = list(csv.DictReader(f))
    return {"ts": run_dir.name, "steps": steps}


@app.get("/api/runs")
def list_runs():
    if not DATA_DIR.exists():
        return jsonify([])
    dirs = sorted(DATA_DIR.glob("*T*"), reverse=True)
    return jsonify([_read_run(d) for d in dirs])


@app.get("/api/runs/<ts>")
def get_run(ts: str):
    run_dir = DATA_DIR / ts
    if not run_dir.is_dir():
        abort(404)
    return jsonify(_read_run(run_dir))


@app.get("/")
def serve_gui():
    return send_from_directory(".", "index.html")

def run_api():
    app.run(host="0.0.0.0", port=PORT)

