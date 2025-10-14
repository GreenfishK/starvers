# scripts/visualize_ostrich.py
import os, sys, logging
from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt
from datetime import datetime
from datetime import timezone
import tomli
import math
import numpy as np

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
def read_csv(path: Path) -> pd.DataFrame | None:
    if not path.exists():
        logging.warning(f"Missing file: {path}")
        return None

    # Try a few increasingly-forgiving parses
    attempts = [
        # 1) Fast path: C engine, UTF-8
        dict(sep=";", engine="c", encoding="utf-8", dtype_backend=None),
        # 2) Python engine (more flexible with weird quoting)
        dict(sep=";", engine="python", encoding="utf-8"),
        # 3) Skip bad lines
        dict(sep=";", engine="python", encoding="utf-8", on_bad_lines="skip"),
    ]

    for i, kwargs in enumerate(attempts, start=1):
        try:
            df = pd.read_csv(path, **kwargs)
            # If delimiter was wrong you sometimes get a single wide column – detect & retry with regex sep
            if df.shape[1] == 1:
                df = pd.read_csv(path, sep=r";\s*", engine=kwargs.get("engine","python"),
                                 encoding=kwargs.get("encoding","utf-8"),
                                 on_bad_lines=kwargs.get("on_bad_lines",None))
            return df
        except Exception as e:
            logging.error(f"Attempt {i} failed for {path}: {e}")

    # 4) Last resort: read text, strip NULs/odd bytes, then parse
    try:
        with open(path, "r", encoding="utf-8", errors="replace", newline="") as fh:
            text = fh.read().replace("\x00", "")
        # normalize newlines and re-parse
        text = text.replace("\r\n", "\n").replace("\r", "\n")
        return pd.read_csv(io.StringIO(text), sep=";", engine="python", on_bad_lines="skip")
    except Exception as e:
        print("failed to read")
        logging.error(f"Failed to read {path} after all attempts: {e}")
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
def plot_ingestion(ingestion_df: pd.DataFrame, out_dir: Path, dataset:str):
    if ingestion_df is None or ingestion_df.empty:
        return
    # Time per version
    ingestion_df["ingestion_time_s"] = pd.to_numeric(ingestion_df['durationms'], errors="coerce") / 1_000.0
    markevery = math.ceil(len(ingestion_df['version'])/150)

    plt.figure()
    plt.plot(ingestion_df["version"], ingestion_df["ingestion_time_s"], linestyle='none', marker="o", markersize=7, markerfacecolor='none',
             markeredgewidth=1, drawstyle='steps', linewidth=0.5, color='black', markevery=markevery)
    
    ingestiontime_max = float(np.nanmax(ingestion_df["ingestion_time_s"].to_numpy(dtype=float)))
    exp = math.floor(math.log10(ingestiontime_max))
    for m in (1, 2, 5, 10):
        candidate = m * (10 ** exp)
        if candidate >= ingestiontime_max:
            upper = candidate
            break
    upper = min(upper, 1000)

    ax = plt.gca()
    ax.set_yscale("log")
    ax.set_ylim(1e-3, 1e-0)

    plt.xlabel("Version")
    plt.ylabel("Ingestion time (s)")
    plt.title(f"Ingestion duration per version – {dataset}")
    safe_save(out_dir / f"{dataset}_ingestion_time.png")

    # Cumulative size
    if "accsize" in ingestion_df.columns:
        ingestion_df["size_mb"] = pd.to_numeric(ingestion_df['accsize'], errors="coerce") / (1024 ** 2)
        plt.figure()
        plt.plot(ingestion_df["version"], ingestion_df["size_mb"], linestyle='dashed', marker="o", markersize=7, markerfacecolor='none',
             markeredgewidth=1, drawstyle='steps', linewidth=0.5, color='black', markevery=markevery)
        
        upper = 0
        size_max = float(np.nanmax(ingestion_df["size_mb"].to_numpy(dtype=float)))
        exp = math.floor(math.log10(size_max))
        for m in (1, 2, 5, 10):
           candidate = m * (10 ** exp)
           if candidate >= size_max:
               upper = candidate
               break
        upper = min(upper, 1000)
    
        ax = plt.gca()
        ax.set_yscale("linear")
        ax.set_ylim(0, upper)
    
        plt.xlabel("Version")
        plt.ylabel("Cumulative size (MB)")
        plt.title(f"Cumulative store size – {dataset}")
        safe_save(out_dir / f"{dataset}_cumulative_size.png")

def plot_vm(vm_df_p: pd.DataFrame, vm_df_po: pd.DataFrame, sparql_df: pd.DataFrame, out_dir: Path, tag: str):
    if (vm_df_p is None and vm_df_po is None) or (vm_df_p.empty and vm_df_po.empty):
        print(vm_df_p)
        print(vm_df_po)
        print("some df empty")
        return
    
    # Expect columns: patch, offset, limit, count-ms, lookup-mus, results
    vm_df = pd.concat([vm_df_p, vm_df_po], ignore_index=True)
    means = vm_df.groupby('patch', as_index=False)['lookup-mus'].mean()
    means["execution_time_total"] = pd.to_numeric(means['lookup-mus'], errors="coerce") / 1_000_000.0
    markevery = math.ceil(len(means['patch'])/60)
    plt.figure()
    plt.plot(means["patch"], means["execution_time_total"], linestyle='none', marker="o", markersize=7, markerfacecolor='none',
             markeredgewidth=1, drawstyle='steps', linewidth=0.5, color='black', markevery=markevery, label='Triple pattern')
    
    if sparql_df is not None and not sparql_df.empty:
        sparql_med = sparql_df.groupby('snapshot', as_index=False)['execution_time'].median().rename(
            columns={'snapshot': 'patch', 'execution_time': 'sparql_seconds'}
        )
        plt.plot(sparql_med["patch"], sparql_med["sparql_seconds"], linestyle='none', marker="*", markersize=7, markerfacecolor='none',
            markeredgewidth=1, drawstyle='steps', linewidth=0.5, color='black', markevery=markevery, label='SPARQL'
        )


    ax = plt.gca()
    ax.set_yscale("log")
    ax.set_ylim(1e-4, 1e-2)
    plt.xlabel("Version")
    plt.ylabel("Median lookup (s)")
    plt.title(f"VM – median lookup over versions – {tag}")
    plt.legend(loc='best')
    plt.tight_layout()
    safe_save(out_dir / f"{tag}_vm_median_lookup.png")

def plot_dm(dm_df_p: pd.DataFrame, dm_df_po: pd.DataFrame,  out_dir: Path, tag: str):
    if (dm_df_p is None and dm_df_po is None) or (dm_df_p.empty and dm_df_po.empty):
        return
    
    dm_df = pd.concat([dm_df_p, dm_df_po], ignore_index=True)
    # Expect columns: patch_start, patch_end, offset, limit, count-ms, lookup-mus, results
    if "patch_end" not in dm_df.columns:
        logging.warning("DM CSV has no 'patch_end' column; skipping DM plot.")
        return
    means = dm_df.groupby('patch_end', as_index=False)['lookup-mus'].mean()
    means["execution_time_total"] = pd.to_numeric(means['lookup-mus'], errors="coerce") / 1_000_000.0
    markevery = math.ceil(len(means['patch_end'])/60)
    plt.figure()
    plt.plot(means["patch_end"], means["execution_time_total"], linestyle='none', marker="o", markersize=7, markerfacecolor='none',
             markeredgewidth=1, drawstyle='steps', linewidth=0.5, color='black', markevery=markevery)
    
    ax = plt.gca()
    ax.set_yscale("log")
    ax.set_ylim(1e-4, 1e-2)

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
def visualize(dataset: str, measurements_path: str):
    """
    results_dir contains: <prefix>_insertion.csv, <prefix>_vm.csv, <prefix>_dm.csv, <prefix>_vq.csv
    """
    logging.info(f"Processing {dataset}")

    csv_path = f"{measurements_path}/{dataset}"

    ingestion_csv = Path(f"{csv_path}/ingestion.csv")
    vm_csv_p      = Path(f"{csv_path}/basic/lookup_queries_p_vm.csv")
    vm_csv_po     = Path(f"{csv_path}/basic/lookup_queries_po_vm.csv")
    dm_csv_p      = Path(f"{csv_path}/basic/lookup_queries_p_dm.csv")
    dm_csv_po     = Path(f"{csv_path}/basic/lookup_queries_po_dm.csv")
    sparql_csv    = Path(f"{measurements_path}/time.csv")
    #vq_csv        = Path(f"{csv_path}/basic/lookup_queries_{prefix}_vq.csv")
    #vq_csv        = Path(f"{csv_path}/basic/lookup_queries_{prefix}_vq.csv")

    ingestion_df = read_csv(ingestion_csv)
    vm_df_p      = read_csv(vm_csv_p)
    vm_df_po     = read_csv(vm_csv_po)
    dm_df_p      = read_csv(dm_csv_p)
    dm_df_po     = read_csv(dm_csv_po)
    sparql_df    = read_csv(sparql_csv)
    #vq_df        = read_csv(vq_csv)

    tag = f"{dataset}"
    out_dir = Path("/ostrich_eval/output/figures").resolve()

    sparql_lookup_df = sparql_df[(sparql_df['dataset'] == dataset) & (sparql_df['query_set'] == 'lookup')]

    # Plots
    plot_ingestion(ingestion_df, out_dir, dataset)
    plot_vm(vm_df_p, vm_df_po, sparql_lookup_df, out_dir, tag)
    plot_dm(dm_df_p, dm_df_po, out_dir, tag)
    #plot_vq(vq_df, ingestion_df, out_dir, tag)

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

    measurements_path = f"/ostrich_eval/output/measurements"

    if dataset in ["bearb_day", "bearb_hour"]:
        visualize(dataset, measurements_path)