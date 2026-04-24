#!/usr/bin/env python3
"""
run_starvers_eval.py
Orchestrates the starvers evaluation pipeline by running scripts directly inside the container.

Steps (in order):
  1  download
  2  preprocess_data
  3  construct_datasets
  4  ingest
  5  construct_queries
  6  evaluate
  7  visualize

Usage:
  python run_starvers_eval.py run all
  python run_starvers_eval.py run step <step_number_or_name>
  python run_starvers_eval.py run from <step_number_or_name>
  python run_starvers_eval.py continue
  python run_starvers_eval.py delete --older-than <YYYYMMDDThhmmss>
  python run_starvers_eval.py list
  python run_starvers_eval.py gui

"""

import argparse
import csv
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.append(str(Path("/starvers_eval/gui").resolve()))
from api import run_api

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

BASE_DATA_DIR = Path("/starvers_eval/data")

STEPS: list[dict] = [
    {"number": 1, "name": "download",           "script": Path("/starvers_eval/scripts/1_download/download_data.sh")},
    {"number": 2, "name": "preprocess_data",    "script": Path("/starvers_eval/scripts/2_preprocess_data/preprocess_data.py")},
    {"number": 3, "name": "construct_datasets", "script": Path("/starvers_eval/scripts/3_construct_datasets/construct_datasets.py")},
    {"number": 4, "name": "ingest",             "script": Path("/starvers_eval/scripts/4_ingest/ingest.py")},
    {"number": 5, "name": "construct_queries",  "script": Path("/starvers_eval/scripts/5_construct_queries/construct_queries.py")},
    {"number": 6, "name": "evaluate",           "script": Path("/starvers_eval/scripts/6_evaluate/evaluate.py")},
    {"number": 7, "name": "visualize",          "script": Path("/starvers_eval/scripts/7_visualize/visualize.py")},
]

EXECUTION_CSV = "execution.csv"
CSV_FIELDS = ["step_number", "step_name", "start_time", "end_time", "status"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H:%M:%S.%f")[:-3]


def _make_run_dir() -> Path:
    """Create and return a new timestamped run directory."""
    BASE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H-%M-%S.%f")[:-3]
    d = BASE_DATA_DIR / ts
    d.mkdir(parents=True, exist_ok=True)
    return d


def _last_run_dir() -> Path | None:
    """Return the most recently created run directory."""
    dirs = sorted(BASE_DATA_DIR.glob("*T*"), reverse=True)
    return dirs[0] if dirs else None


def _resolve_step(identifier: str) -> dict:
    """Resolve a step by number or name."""
    for step in STEPS:
        if str(step["number"]) == identifier or step["name"] == identifier:
            return step
    raise ValueError(
        f"Unknown step: {identifier!r}. "
        f"Valid steps: {[s['name'] for s in STEPS]}"
    )


def _read_csv(run_dir: Path) -> list[dict]:
    csv_path = run_dir / EXECUTION_CSV
    if not csv_path.exists():
        return []
    with open(csv_path, newline="") as f:
        return list(csv.DictReader(f))


def _write_csv(run_dir: Path, rows: list[dict]) -> None:
    csv_path = run_dir / EXECUTION_CSV
    with open(csv_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _update_row(rows: list[dict], step: dict, **kwargs) -> list[dict]:
    for row in rows:
        if row["step_number"] == str(step["number"]):
            row.update(kwargs)
            return rows
    rows.append({
        "step_number": str(step["number"]),
        "step_name": step["name"],
        "start_time": kwargs.get("start_time", ""),
        "end_time": kwargs.get("end_time", ""),
        "status": kwargs.get("status", ""),
    })
    return rows


def _run_step(step: dict, run_dir: Path) -> int:
    """Run a single step script and return its exit code."""
    script_path: Path = step["script"]

    if not script_path.exists():
        print(f"[starvers_eval] ERROR: Script not found: {script_path}", file=sys.stderr)
        return 1

    suffix = script_path.suffix
    if suffix == ".sh":
        cmd = ["bash", str(script_path)]
    elif suffix == ".py":
        cmd = ["python", "-u", str(script_path)]
    else:
        print(f"[starvers_eval] ERROR: Unsupported script type: {script_path}", file=sys.stderr)
        return 1

    print(f"\n[starvers_eval] Running step: {step['name']}")
    print(f"[starvers_eval] Command: {' '.join(cmd)}\n")

    # Inherit the full environment so scripts can read $datasets, $policies, etc.
    result = subprocess.run(cmd, env=os.environ.copy())
    return result.returncode


# ---------------------------------------------------------------------------
# Core pipeline runner
# ---------------------------------------------------------------------------

def execute_steps(steps_to_run: list[dict], run_dir: Path) -> None:
    # Crucial for scripts to know where to read/write data for this run
    os.environ["RUN_DIR"] = str(run_dir) 

    # Set up directory structure
    for dir_path in [
        f"{os.environ['RUN_DIR']}/databases",
        f"{os.environ['RUN_DIR']}/output/logs",
        f"{os.environ['RUN_DIR']}/output/measurements",
        f"{os.environ['RUN_DIR']}/output/result_sets",
        f"{os.environ['RUN_DIR']}/output/figures",
        f"{os.environ['RUN_DIR']}/rawdata",
        f"{os.environ['RUN_DIR']}/queries/raw_queries/beara/low",
        f"{os.environ['RUN_DIR']}/queries/raw_queries/beara/high",
        f"{os.environ['RUN_DIR']}/queries/raw_queries/bearb/lookup",
        f"{os.environ['RUN_DIR']}/queries/raw_queries/bearb/join",
        f"{os.environ['RUN_DIR']}/queries/raw_queries/bearc/complex",
        f"{os.environ['RUN_DIR']}/queries/raw_queries/orkg/complex",
    ]:
        os.makedirs(dir_path, exist_ok=True)

    rows = _read_csv(run_dir)

    for step in steps_to_run:
        start = _now_ts()
        rows = _update_row(rows, step, start_time=start, end_time="", status="running")
        _write_csv(run_dir, rows)

        rc = _run_step(step, run_dir)

        end = _now_ts()
        status = "success" if rc == 0 else "failed"
        rows = _update_row(rows, step, start_time=start, end_time=end, status=status)
        _write_csv(run_dir, rows)

        if rc != 0:
            print(
                f"\n[starvers_eval] Step '{step['name']}' FAILED (exit code {rc}). "
                f"Execution recorded in {run_dir / EXECUTION_CSV}. "
                f"Use 'continue' to resume.",
                file=sys.stderr,
            )
            sys.exit(rc)

    print(
        f"\n[starvers_eval] All steps completed successfully. "
        f"Execution log: {run_dir / EXECUTION_CSV}"
    )


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

def cmd_run(args) -> None:
    # 'run all' and 'run from' own a full pipeline run → new timestamped dir.
    # 'run step' is a targeted one-off → reuse the last run dir so that
    # continue/list stay coherent, or create one if no prior run exists.
    if args.subcommand == "step":
        run_dir = _last_run_dir() or _make_run_dir()
        print(f"[starvers_eval] Run directory: {run_dir}")
        step = _resolve_step(args.step_id)
        execute_steps([step], run_dir)

    elif args.subcommand == "all":
        run_dir = _make_run_dir()
        print(f"[starvers_eval] Run directory: {run_dir}")
        execute_steps(STEPS, run_dir)

    elif args.subcommand == "from":
        run_dir = _last_run_dir()
        print(f"[starvers_eval] Run directory: {run_dir}")
        step = _resolve_step(args.step_id)
        idx = STEPS.index(step)
        execute_steps(STEPS[idx:], run_dir)

    elif args.subcommand == "step_at":
        run_dir = BASE_DATA_DIR / args.timestamp
        if not run_dir.exists():
            print(f"[starvers_eval] ERROR: Run directory not found for timestamp: {args.timestamp}", file=sys.stderr)
            sys.exit(1)
        print(f"[starvers_eval] Run directory: {run_dir}")
        step = _resolve_step(args.step_id)
        execute_steps([step], run_dir)

    else:
        print(f"Unknown run subcommand: {args.subcommand}", file=sys.stderr)
        sys.exit(1)


def cmd_continue(args) -> None:
    run_dir = _last_run_dir()
    if run_dir is None:
        print("[starvers_eval] No previous runs found.", file=sys.stderr)
        sys.exit(1)

    rows = _read_csv(run_dir)
    finished = {r["step_name"] for r in rows if r.get("status") == "success"}
    remaining = [s for s in STEPS if s["name"] not in finished]

    if not remaining:
        print("[starvers_eval] All steps already completed successfully.")
        return

    print(f"[starvers_eval] Continuing from step '{remaining[0]['name']}' in {run_dir}")
    execute_steps(remaining, run_dir)


def cmd_delete(args) -> None:
    cutoff_str: str = args.older_than
    try:
        cutoff = datetime.strptime(cutoff_str, "%Y%m%dT%H%M%S")
    except ValueError:
        print(
            f"[starvers_eval] Invalid timestamp format. "
            f"Expected YYYYMMDDThhmmss, got: {cutoff_str}",
            file=sys.stderr,
        )
        sys.exit(1)

    deleted = []
    for d in BASE_DATA_DIR.glob("*T*"):
        try:
            dir_ts_str = d.name.replace(":", "").replace(".", "")[:15]
            dir_ts = datetime.strptime(dir_ts_str, "%Y%m%dT%H%M%S")
        except ValueError:
            continue
        if dir_ts < cutoff:
            import shutil
            shutil.rmtree(d)
            deleted.append(d)

    if deleted:
        print(f"[starvers_eval] Deleted {len(deleted)} run(s):")
        for d in deleted:
            print(f"  {d}")
    else:
        print("[starvers_eval] No runs older than the given timestamp found.")


def cmd_list(args) -> None:
    dirs = sorted(BASE_DATA_DIR.glob("*T*"), reverse=True)
    if not dirs:
        print("[starvers_eval] No runs found.")
        return
    print(f"{'Run timestamp':<30} {'Steps completed'}")
    print("-" * 60)
    for d in dirs:
        rows = _read_csv(d)
        completed = sum(1 for r in rows if r.get("status") == "success")
        print(f"{d.name:<30} {completed}/{len(STEPS)}")


def cmd_gui(args) -> None:
    # execute: /starvers_eval/evaluation/gui/api.py
    run_api()


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Starvers evaluation pipeline orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Execute pipeline steps")
    run_sub = run_p.add_subparsers(dest="subcommand", required=True)
    run_sub.add_parser("all", help="Run the full pipeline")
    step_p = run_sub.add_parser("step", help="Run a single step")
    step_p.add_argument("step_id", help="Step number (1-7) or name")
    from_p = run_sub.add_parser("from", help="Run from a specific step onwards")
    from_p.add_argument("step_id", help="Step number (1-7) or name")

    # add argument after step <step_id> at <timestamp> to run a step for a specific run
    after_p = run_sub.add_parser("step_at", help="Run a step for a specific run")
    after_p.add_argument("step_id", help="Step number (1-7) or name")
    after_p.add_argument("timestamp", help="Run timestamp")


    sub.add_parser("continue", help="Continue the last failed/interrupted run")

    del_p = sub.add_parser("delete", help="Delete runs older than a given timestamp")
    del_p.add_argument(
        "--older-than", required=True, metavar="YYYYMMDDThhmmss",
        help="Delete runs older than this timestamp",
    )

    sub.add_parser("list", help="List all runs and their status")

    sub.add_parser("gui", help="Run the evaluation GUI")  

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "run": cmd_run,
        "continue": cmd_continue,
        "delete": cmd_delete,
        "list": cmd_list,
        "gui": cmd_gui
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
