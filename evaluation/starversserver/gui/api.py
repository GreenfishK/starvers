"""
api.py – Lightweight Flask backend for the Starversserver Evaluation GUI.

Serves:
  GET /evaluation/starversserver/api/runs         -> list of runs (newest first)
  GET /evaluation/starversserver/api/runs/<ts>    -> detail for one run
  GET /evaluation/starversserver/                 -> index.html GUI

Run (inside container):
  python api.py

Environment variables:
  DATA_DIR   – path where run directories live (default: /mnt/data/starversserver_eval)
  PORT       – port to bind (default: 8081)
"""

import csv
import os
from pathlib import Path
from flask import Flask, jsonify, send_from_directory, abort

app = Flask(__name__, static_folder=".")

DATA_DIR = Path(os.environ.get("DATA_DIR", "/mnt/data/starversserver_eval"))
PORT = int(os.environ.get("PORT", 8081))


def _read_run(run_dir: Path) -> dict:
    csv_path = run_dir / "execution.csv"
    steps = []
    if csv_path.exists():
        with open(csv_path, newline="") as f:
            steps = list(csv.DictReader(f))
    return {"ts": run_dir.name, "steps": steps}


@app.get("/evaluation/starversserver/api/runs")
def list_runs():
    if not DATA_DIR.exists():
        return jsonify([])
    dirs = sorted(DATA_DIR.glob("*T*"), reverse=True)
    return jsonify([_read_run(d) for d in dirs])


@app.get("/evaluation/starversserver/api/runs/<ts>")
def get_run(ts: str):
    run_dir = DATA_DIR / ts
    if not run_dir.is_dir():
        abort(404)
    return jsonify(_read_run(run_dir))


@app.get("/evaluation/starversserver/")
@app.get("/evaluation/starversserver")
def serve_gui():
    return send_from_directory(".", "index.html")


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=PORT)
