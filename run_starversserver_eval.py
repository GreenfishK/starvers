#!/usr/bin/env python3
"""
run_starversserver_eval.py
Orchestrates the starversserver evaluation pipeline via docker-compose services.

Steps (in order):
  1  compute
  2  create_plots

Usage:
  python run_starversserver_eval.py run all
  python run_starversserver_eval.py run step <step_number_or_name>
  python run_starversserver_eval.py run from <step_number_or_name>
  python run_starversserver_eval.py continue
  python run_starversserver_eval.py delete --older-than <YYYYMMDDThhmmss>
  python run_starversserver_eval.py list
"""

import argparse
import csv
import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

COMPOSE_FILE = "starversserver.eval.compose.yml"
ENV_FILE = "starversserver.eval.env"
BASE_DATA_DIR = Path("/mnt/data/starversserver_eval")

STEPS: list[dict] = [
    {"number": 1, "name": "compute",      "service": "compute"},
    {"number": 2, "name": "create_plots", "service": "create_plots"},
]

EXECUTION_CSV = "execution.csv"
CSV_FIELDS = ["step_number", "step_name", "start_time", "end_time", "status"]

# ---------------------------------------------------------------------------
# Helpers (identical structure to run_starvers_eval.py)
# ---------------------------------------------------------------------------

def _now_ts() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H:%M:%S.%f")[:-3]


def _run_dir() -> Path:
    BASE_DATA_DIR.mkdir(parents=True, exist_ok=True)
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H:%M:%S.%f")[:-3]
    d = BASE_DATA_DIR / ts
    d.mkdir(parents=True, exist_ok=True)
    return d


def _last_run_dir() -> Path | None:
    dirs = sorted(BASE_DATA_DIR.glob("*T*"), reverse=True)
    return dirs[0] if dirs else None


def _resolve_step(identifier: str) -> dict:
    for step in STEPS:
        if str(step["number"]) == identifier or step["name"] == identifier:
            return step
    raise ValueError(f"Unknown step: {identifier!r}. "
                     f"Valid steps: {[s['name'] for s in STEPS]}")


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


def _run_service(service: str) -> int:
    """Run a single docker-compose service (with env_file) and return exit code."""
    cmd = [
        "docker", "compose",
        "-f", COMPOSE_FILE,
        "--env-file", ENV_FILE,
        "run", "--rm", service,
    ]
    print(f"\n[starversserver_eval] Running service: {service}")
    print(f"[starversserver_eval] Command: {' '.join(cmd)}\n")
    result = subprocess.run(cmd)
    return result.returncode


# ---------------------------------------------------------------------------
# Core pipeline runner
# ---------------------------------------------------------------------------

def execute_steps(steps_to_run: list[dict], run_dir: Path) -> None:
    rows = _read_csv(run_dir)

    for step in steps_to_run:
        start = _now_ts()
        rows = _update_row(rows, step, start_time=start, end_time="", status="running")
        _write_csv(run_dir, rows)

        rc = _run_service(step["service"])

        end = _now_ts()
        status = "success" if rc == 0 else "failed"
        rows = _update_row(rows, step, start_time=start, end_time=end, status=status)
        _write_csv(run_dir, rows)

        if rc != 0:
            print(f"\n[starversserver_eval] Step '{step['name']}' FAILED (exit code {rc}). "
                  f"Execution recorded in {run_dir / EXECUTION_CSV}. "
                  f"Use 'continue' to resume.", file=sys.stderr)
            sys.exit(rc)

    print(f"\n[starversserver_eval] All steps completed successfully. "
          f"Execution log: {run_dir / EXECUTION_CSV}")


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

def cmd_run(args) -> None:
    run_dir = _run_dir()
    print(f"[starversserver_eval] Run directory: {run_dir}")

    if args.subcommand == "all":
        execute_steps(STEPS, run_dir)

    elif args.subcommand == "step":
        step = _resolve_step(args.step_id)
        execute_steps([step], run_dir)

    elif args.subcommand == "from":
        step = _resolve_step(args.step_id)
        idx = STEPS.index(step)
        execute_steps(STEPS[idx:], run_dir)

    else:
        print(f"Unknown run subcommand: {args.subcommand}", file=sys.stderr)
        sys.exit(1)


def cmd_continue(args) -> None:
    run_dir = _last_run_dir()
    if run_dir is None:
        print("[starversserver_eval] No previous runs found.", file=sys.stderr)
        sys.exit(1)

    rows = _read_csv(run_dir)
    finished = {r["step_name"] for r in rows if r.get("status") == "success"}
    remaining = [s for s in STEPS if s["name"] not in finished]

    if not remaining:
        print("[starversserver_eval] All steps already completed successfully.")
        return

    first_unfinished = remaining[0]
    print(f"[starversserver_eval] Continuing from step '{first_unfinished['name']}' in {run_dir}")
    execute_steps(remaining, run_dir)


def cmd_delete(args) -> None:
    cutoff_str: str = args.older_than
    try:
        cutoff = datetime.strptime(cutoff_str, "%Y%m%dT%H%M%S")
    except ValueError:
        print(f"[starversserver_eval] Invalid timestamp format. Expected YYYYMMDDThhmmss, got: {cutoff_str}",
              file=sys.stderr)
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
        print(f"[starversserver_eval] Deleted {len(deleted)} run(s):")
        for d in deleted:
            print(f"  {d}")
    else:
        print("[starversserver_eval] No runs older than the given timestamp found.")


def cmd_list(args) -> None:
    dirs = sorted(BASE_DATA_DIR.glob("*T*"), reverse=True)
    if not dirs:
        print("[starversserver_eval] No runs found.")
        return
    print(f"{'Run timestamp':<30} {'Steps completed'}")
    print("-" * 60)
    for d in dirs:
        rows = _read_csv(d)
        completed = sum(1 for r in rows if r.get("status") == "success")
        total = len(STEPS)
        print(f"{d.name:<30} {completed}/{total}")


# ---------------------------------------------------------------------------
# Argument parsing
# ---------------------------------------------------------------------------

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Starversserver evaluation pipeline orchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )
    sub = parser.add_subparsers(dest="command", required=True)

    run_p = sub.add_parser("run", help="Execute pipeline steps")
    run_sub = run_p.add_subparsers(dest="subcommand", required=True)
    run_sub.add_parser("all", help="Run the full pipeline")
    step_p = run_sub.add_parser("step", help="Run a single step")
    step_p.add_argument("step_id", help="Step number (1-2) or name")
    from_p = run_sub.add_parser("from", help="Run from a specific step onwards")
    from_p.add_argument("step_id", help="Step number (1-2) or name")

    sub.add_parser("continue", help="Continue the last failed/interrupted run")

    del_p = sub.add_parser("delete", help="Delete runs older than a given timestamp")
    del_p.add_argument("--older-than", required=True, metavar="YYYYMMDDThhmmss",
                       help="Delete runs older than this timestamp")

    sub.add_parser("list", help="List all runs and their status")

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    dispatch = {
        "run": cmd_run,
        "continue": cmd_continue,
        "delete": cmd_delete,
        "list": cmd_list,
    }
    dispatch[args.command](args)


if __name__ == "__main__":
    main()
