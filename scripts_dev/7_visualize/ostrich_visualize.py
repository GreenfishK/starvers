# scripts/visualize_ostrich.py
import os, sys, logging
from pathlib import Path
import itertools
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timezone
import tomli

# ---------------------------------------
# Params (CLI)
# ---------------------------------------
# Usage examples:
#   python scripts/visualize_ostrich.py data/bearb_day/results bearb_p
#   python scripts/visualize_ostrich.py data/bearb_day/results "bearb_p,bearb_po"
#   python scripts/visualize_ostrich.py "data/bearb_day/results;data/bearb_hour/results" "bearb_p,bearb_po"
#
# Arg1: one or multiple results dirs, separated by ';'
# Arg2: one or multiple prefixes (file prefixes), separated by ','
#if len(sys.argv) != 3:
#    print("Usage: python scripts/visualize_ostrich.py <results_dir[;results_dir2...]> <prefix[,prefix2...]>")
#    sys.exit(1)

RESULT_DIRS = [Path("data/bearb_day/results").resolve()] #[Path(p).resolve() for p in sys.argv[1].split(";") if p]
PREFIXES = ["bearb_p", "bearb_po"] #[p.strip() for p in sys.argv[2].split(",") if p.strip()]

# ---------------------------------------
# Helpers
# ---------------------------------------
def read_csv(path: Path):
    if not path.exists():
        logging.warning(f"Missing file: {path}")
        return None
    try:
        return pd.read_csv(path, delimiter=";")
    except Exception as e:
        logging.error(f"Failed to read {path}: {e}")
        return None

def safe_save(figpath: Path):
    figpath.parent.mkdir(parents=True, exist_ok=True)
    plt.tight_layout()
    plt.savefig(figpath, dpi=160)
    plt.close()

def detect_last_version_from_insertion(insertion_df: pd.DataFrame) -> int:
    if "version" not in insertion_df.columns or insertion_df.empty:
        return 0
    return int(insertion_df["version"].max())

# ---------------------------------------
# Plotters
# ---------------------------------------
def plot_ingestion(insertion_df: pd.DataFrame, out_dir: Path, dataset:str):
    if insertion_df is None or insertion_df.empty:
        return
    # Time per version
    plt.figure()
    plt.plot(insertion_df["version"], insertion_df["durationms"] / 1000.0, marker="o")
    plt.xlabel("Version")
    plt.ylabel("Insertion time (s)")
    plt.title(f"Ingestion duration per version – {dataset}")
    safe_save(out_dir / f"{dataset}_ingestion_time.png")

    # Cumulative size
    if "accsize" in insertion_df.columns:
        plt.figure()
        plt.plot(insertion_df["version"], insertion_df["accsize"] / (1024 ** 2), marker="o")
        plt.xlabel("Version")
        plt.ylabel("Cumulative size (MB)")
        plt.title(f"Cumulative store size – {dataset}")
        safe_save(out_dir / f"{dataset}_cumulative_size.png")

def plot_vm(vm_df: pd.DataFrame, out_dir: Path, tag: str):
    if vm_df is None or vm_df.empty:
        return
    # Expect columns: patch, offset, limit, count-ms, lookup-mus, results
    g = vm_df.groupby("patch")["lookup-mus"].median().reset_index()
    plt.figure()
    plt.plot(g["patch"], g["lookup-mus"]/1000.0, marker="o")
    plt.xlabel("Version")
    plt.ylabel("Median lookup (ms)")
    plt.title(f"VM – median lookup over versions – {tag}")
    safe_save(out_dir / f"{tag}_vm_median_lookup.png")

def plot_dm(dm_df: pd.DataFrame, out_dir: Path, tag: str):
    if dm_df is None or dm_df.empty:
        return
    # Expect columns: patch_start, patch_end, offset, limit, count-ms, lookup-mus, results
    if "patch_end" not in dm_df.columns:
        logging.warning("DM CSV has no 'patch_end' column; skipping DM plot.")
        return
    g = dm_df.groupby("patch_end")["lookup-mus"].median().reset_index()
    plt.figure()
    plt.plot(g["patch_end"], g["lookup-mus"]/1000.0, marker="o")
    plt.xlabel("Version (end)")
    plt.ylabel("Median lookup (ms)")
    plt.title(f"DM (0→v) – median lookup over versions – {tag}")
    safe_save(out_dir / f"{tag}_dm_median_lookup.png")

def plot_vq(vq_df: pd.DataFrame, insertion_df: pd.DataFrame, out_dir: Path, tag: str):
    # VQ is typically executed only at the final version; many builds don't print a 'patch' col.
    if vq_df is None or vq_df.empty or insertion_df is None or insertion_df.empty:
        return
    last_v = detect_last_version_from_insertion(insertion_df)
    med = float(vq_df["lookup-mus"].median()) / 1000.0
    plt.figure()
    plt.scatter([last_v], [med])
    plt.axvline(x=last_v, linestyle="--", linewidth=1)
    plt.xlabel("Version")
    plt.ylabel("Median lookup (ms)")
    plt.title(f"VQ – median lookup (final version) – {tag}")
    safe_save(out_dir / f"{tag}_vq_median_lookup.png")



# ---------------------------------------
# Driver
# ---------------------------------------
def visualize(dataset: str, csv_path: str, prefix=None):
    """
    results_dir contains: <prefix>_insertion.csv, <prefix>_vm.csv, <prefix>_dm.csv, <prefix>_vq.csv
    """
    logging.info(f"Processing {dataset} for prefix {prefix}")

    ingestion_csv = Path(f"{csv_path}/ingestion.csv")
    vm_csv        = Path(f"{csv_path}/basic/lookup_queries_{prefix}_vm.csv")
    dm_csv        = Path(f"{csv_path}/basic/lookup_queries_{prefix}_dm.csv")
    vq_csv        = Path(f"{csv_path}/basic/lookup_queries_{prefix}_vq.csv")

    ingestion_df = read_csv(ingestion_csv)
    vm_df        = read_csv(vm_csv)
    dm_df        = read_csv(dm_csv)
    vq_df        = read_csv(vq_csv)

    tag = f"{dataset}_{prefix}"
    out_dir = Path("/ostrich_eval/output/figures").resolve()

    # Plots
    plot_ingestion(ingestion_df, out_dir, dataset)
    plot_vm(vm_df, out_dir, tag)
    plot_dm(dm_df, out_dir, tag)
    plot_vq(vq_df, ingestion_df, out_dir, tag)

############################################# Logging #############################################
if not os.path.exists('/ostrich_eval/output/logs/visualize'):
    os.makedirs('/ostrich_eval/output/logs/visualize')
with open('/ostrich_eval/output/logs/visualize/visualize.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/ostrich_eval/output/logs/visualize/visualize.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

############################################# Parameters #############################################
datasets = sys.argv[1].split(" ")

in_frm = "nt"
LOCAL_TIMEZONE = datetime.now(timezone.utc).astimezone().tzinfo
with open("/ostrich_eval/configs", mode="rb") as config_file:
    eval_setup = tomli.load(config_file)
dataset_versions = {dataset: infos['snapshot_versions'] for dataset, infos in eval_setup['datasets'].items()}
allowed_datasets = list(dataset_versions.keys())



############################################# Start procedure #############################################
for dataset in datasets:
    if dataset not in allowed_datasets:
        print("Dataset must be one of: ", allowed_datasets, "but is: {0}".format(dataset))
        break
    print("Vizualizing dataset for {0}".format(dataset))

    csv_path = f"/ostrich_eval/output/measurements/{dataset}"

    if dataset in ["bearb_day", "bearb_hour"]:
        visualize(dataset, csv_path, "p")
        visualize(dataset, csv_path, "po")
    else:
        visualize(dataset)