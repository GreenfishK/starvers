import itertools
import logging
import math
import os

import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import numpy as np
import pandas as pd
import tomli

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
log_dir = f"{os.environ['RUN_DIR']}/output/logs/visualize"
os.makedirs(log_dir, exist_ok=True)
with open(f"{log_dir}/visualize.txt", "w"):
    pass

logging.basicConfig(
    handlers=[
        logging.FileHandler(f"{log_dir}/visualize.txt", encoding="utf-8", mode="a+"),
        logging.StreamHandler(),
    ],
    format="%(asctime)s %(name)s:%(levelname)s:%(message)s",
    datefmt="%F %A %T",
    level=logging.INFO,
)

# ---------------------------------------------------------------------------
# Environment / path constants
# ---------------------------------------------------------------------------
WORK_DIR     = "/starvers_eval/"
CONFIG_PATH  = f"{WORK_DIR}configs/eval_setup.toml"
RESULTS_TMPL = f"{WORK_DIR}scripts/7_visualize/templates/latex_table_results_tmpl.tex"

RUN_DIR          = os.environ["RUN_DIR"]
measurements_in  = f"{RUN_DIR}/output/measurements/"
figures_out      = f"{RUN_DIR}/output/figures"
tables_out       = f"{RUN_DIR}/output/tables"

os.makedirs(figures_out, exist_ok=True)
os.makedirs(tables_out,  exist_ok=True)

POLICIES      = os.environ.get("policies", "").split()
DATASETS      = os.environ.get("datasets", "").split()
TRIPLE_STORES = os.environ.get("triple_stores", "").split()

pd.set_option("display.max_columns", None)

# ---------------------------------------------------------------------------
# Colour palette — one colour per policy, consistent across all three plots
# ---------------------------------------------------------------------------
_PALETTE = ["#0057A0", "#E06C1A", "#22c55e", "#ef4444", "#a855f7", "#facc15"]

def _policy_color_map(policies: list[str]) -> dict[str, str]:
    return {p: _PALETTE[i % len(_PALETTE)] for i, p in enumerate(policies)}


# ---------------------------------------------------------------------------
# Data loaders (called once and passed in to keep functions independent)
# ---------------------------------------------------------------------------

def _load_performance_data() -> pd.DataFrame:
    """Load and pre-process the query execution time CSV."""
    time_files = [
        f for f in os.listdir(measurements_in)
        if f.startswith("time_") and f.endswith(".csv")
    ]
    if not time_files:
        logging.warning("No time_*.csv file found in %s", measurements_in)
        return pd.DataFrame()

    path = measurements_in + sorted(time_files)[-1]   # use the latest
    logging.info("Loading performance data from %s", path)

    df = pd.read_csv(
        path, delimiter=";", decimal=".",
        dtype={
            "triplestore": str, "dataset": str, "policy": str,
            "query_set": str, "snapshot": str, "query": str,
            "execution_time": "float", "snapshot_creation_time": "float",
            "yn_timeout": "float",
        },
    )
    df["execution_time_total"] = df["execution_time"] + df["snapshot_creation_time"]
    return df


def _load_ingestion_data() -> pd.DataFrame:
    path = measurements_in + "ingestion.csv"
    if not os.path.exists(path):
        logging.warning("ingestion.csv not found in %s", measurements_in)
        return pd.DataFrame()
    df = pd.read_csv(path, delimiter=";", decimal=".")
    df["triplestore"] = df["triplestore"].str.lower()
    return df


# ---------------------------------------------------------------------------
# 1. Query performance  —  line chart, one file per (triplestore, dataset)
# ---------------------------------------------------------------------------

def plot_query_performance(triplestore: str, dataset: str,
                           perf_df: pd.DataFrame, policies: list[str]):
    """
    One subplot per query set (1 or 2).
    X-axis: snapshot version number (10 evenly-spaced ticks).
    Y-axis: mean execution time (log scale).
    Lines: one per policy, colour-coded.
    Output: SVG at figures_out/time_<triplestore>_<dataset>.svg
    """
    colors = _policy_color_map(policies)

    # Filter to this triplestore + dataset
    df = perf_df[
        (perf_df["triplestore"] == triplestore) &
        (perf_df["dataset"]     == dataset)
    ].copy()

    if df.empty:
        logging.warning("No performance data for %s / %s — skipping.", triplestore, dataset)
        return

    query_sets = sorted(df["query_set"].dropna().unique())
    n_qs       = len(query_sets)
    fig_width  = 8 * n_qs if n_qs > 1 else 10

    fig, axes = plt.subplots(1, n_qs, figsize=(fig_width, 5), squeeze=False)

    for col, qs in enumerate(query_sets):
        ax  = axes[0, col]
        qs_df = df[df["query_set"] == qs]

        # Average over queries and runs, grouped by policy × snapshot
        means = (
            qs_df.groupby(["policy", "snapshot"], sort=False)["execution_time_total"]
            .mean()
            .reset_index()
        )

        # Convert snapshot to integer index for a clean numeric x-axis
        all_snapshots = sorted(means["snapshot"].unique(), key=lambda s: int(s) if str(s).isdigit() else s)
        snap_to_idx   = {s: i for i, s in enumerate(all_snapshots)}
        n_snaps       = len(all_snapshots)

        for policy in policies:
            pol_df = means[means["policy"] == policy].copy()
            if pol_df.empty:
                continue
            pol_df["x"] = pol_df["snapshot"].map(snap_to_idx)
            pol_df = pol_df.sort_values("x")
            ax.plot(
                pol_df["x"], pol_df["execution_time_total"],
                label=policy, color=colors[policy], linewidth=1.2,
            )

        # 10 evenly-spaced ticks
        tick_positions = np.linspace(0, n_snaps - 1, min(10, n_snaps), dtype=int)
        tick_labels    = [all_snapshots[i] for i in tick_positions]
        ax.set_xticks(tick_positions)
        ax.set_xticklabels(tick_labels, rotation=45, ha="right", fontsize=8)

        ax.set_yscale("log")
        ax.set_title(f"Query set: {qs}", fontsize=10)
        ax.set_xlabel("Version", fontsize=9)
        ax.set_ylabel("Avg execution time (s)", fontsize=9)
        ax.legend(fontsize=8, loc="upper right")
        ax.grid(axis="y", linestyle="--", linewidth=0.4, alpha=0.6)

    fig.suptitle(
        f"Query performance — {triplestore.upper()} / {dataset.upper()}",
        fontsize=12, fontweight="bold",
    )
    plt.tight_layout()
    out = f"{figures_out}/time_{triplestore}_{dataset}.svg"
    plt.savefig(out, format="svg")
    plt.close()
    logging.info("Saved %s", out)


# ---------------------------------------------------------------------------
# 2. Storage consumption  —  grouped bar chart, one file per (triplestore, dataset)
# ---------------------------------------------------------------------------

def plot_storage(triplestore: str, dataset: str,
                 ingest_df: pd.DataFrame, policies: list[str]):
    """
    Grouped bars: for each policy, side-by-side raw-size and DB-size bars.
    Colour-coded by policy; raw = lighter shade, DB = full colour.
    Output: SVG at figures_out/storage_<triplestore>_<dataset>.svg
    """
    colors = _policy_color_map(policies)

    df = ingest_df[
        (ingest_df["triplestore"] == triplestore) &
        (ingest_df["dataset"]     == dataset)
    ]
    if df.empty:
        logging.warning("No ingestion data for %s / %s — skipping storage plot.", triplestore, dataset)
        return

    # Aggregate across runs
    agg = (
        df.groupby("policy")[["raw_file_size_MiB", "db_files_disk_usage_MiB"]]
        .mean()
        .reindex(policies)          # keep declared policy order
        .dropna(how="all")
    )
    present_policies = agg.index.tolist()
    n = len(present_policies)

    bar_w   = 0.3
    spacing = 0.1
    x       = np.arange(n)

    fig, ax = plt.subplots(figsize=(max(6, n * 1.8), 5))

    for i, policy in enumerate(present_policies):
        raw_val = agg.loc[policy, "raw_file_size_MiB"]
        db_val  = agg.loc[policy, "db_files_disk_usage_MiB"]
        col     = colors[policy]
        # Raw size: lighter shade (alpha 0.45)
        ax.bar(x[i] - (bar_w + spacing) / 2, raw_val, bar_w,
               color=col, alpha=0.45, label=f"{policy} raw")
        # DB size: full colour
        ax.bar(x[i] + (bar_w + spacing) / 2, db_val,  bar_w,
               color=col, alpha=1.0,  label=f"{policy} DB")
        # Value labels
        ax.text(x[i] - (bar_w + spacing) / 2, raw_val, f"{raw_val:.0f}",
                ha="center", va="bottom", fontsize=7)
        ax.text(x[i] + (bar_w + spacing) / 2, db_val,  f"{db_val:.0f}",
                ha="center", va="bottom", fontsize=7)

    ax.set_xticks(x)
    ax.set_xticklabels(present_policies, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Size (MiB)", fontsize=9)
    ax.set_xlabel("Policy", fontsize=9)
    ax.set_title(
        f"Storage — {triplestore.upper()} / {dataset.upper()}",
        fontsize=11, fontweight="bold",
    )

    # Legend: one patch per policy (colour) + raw/DB distinction
    policy_patches = [
        mpatches.Patch(color=colors[p], label=p)
        for p in present_policies
    ]
    raw_patch = mpatches.Patch(facecolor="grey", alpha=0.45, label="Raw size")
    db_patch  = mpatches.Patch(facecolor="grey", alpha=1.0,  label="DB size")
    ax.legend(handles=policy_patches + [raw_patch, db_patch],
              fontsize=8, loc="upper right")

    ax.grid(axis="y", linestyle="--", linewidth=0.4, alpha=0.6)
    plt.tight_layout()
    out = f"{figures_out}/storage_{triplestore}_{dataset}.svg"
    plt.savefig(out, format="svg")
    plt.close()
    logging.info("Saved %s", out)


# ---------------------------------------------------------------------------
# 3. Ingest time  —  boxplot, one file per (triplestore, dataset)
# ---------------------------------------------------------------------------

def plot_ingest_time(triplestore: str, dataset: str,
                     ingest_df: pd.DataFrame, policies: list[str]):
    """
    One boxplot per policy showing the distribution of ingestion times
    over the 10 repeated runs.  Colour-coded by policy.
    Output: SVG at figures_out/ingest_<triplestore>_<dataset>.svg
    """
    colors = _policy_color_map(policies)

    df = ingest_df[
        (ingest_df["triplestore"] == triplestore) &
        (ingest_df["dataset"]     == dataset)
    ]
    if df.empty:
        logging.warning("No ingestion data for %s / %s — skipping ingest plot.", triplestore, dataset)
        return

    present_policies = [p for p in policies if p in df["policy"].unique()]
    n = len(present_policies)
    if n == 0:
        return

    fig, ax = plt.subplots(figsize=(max(6, n * 1.6), 5))

    box_data = [df[df["policy"] == p]["ingestion_time"].dropna().values
                for p in present_policies]

    bp = ax.boxplot(
        box_data,
        positions=range(n),
        widths=0.5,
        patch_artist=True,
        medianprops=dict(color="white", linewidth=2),
    )

    for patch, policy in zip(bp["boxes"], present_policies):
        patch.set_facecolor(colors[policy])
        patch.set_alpha(0.85)

    for element in ["whiskers", "caps", "fliers"]:
        for item in bp[element]:
            item.set_color("black")
            item.set_linewidth(0.8)

    ax.set_xticks(range(n))
    ax.set_xticklabels(present_policies, rotation=30, ha="right", fontsize=9)
    ax.set_ylabel("Ingestion time (s)", fontsize=9)
    ax.set_xlabel("Policy", fontsize=9)
    ax.set_title(
        f"Ingest time — {triplestore.upper()} / {dataset.upper()}",
        fontsize=11, fontweight="bold",
    )

    legend_patches = [
        mpatches.Patch(color=colors[p], label=p) for p in present_policies
    ]
    ax.legend(handles=legend_patches, fontsize=8, loc="upper right")
    ax.grid(axis="y", linestyle="--", linewidth=0.4, alpha=0.6)

    plt.tight_layout()
    out = f"{figures_out}/ingest_{triplestore}_{dataset}.svg"
    plt.savefig(out, format="svg")
    plt.close()
    logging.info("Saved %s", out)


# ---------------------------------------------------------------------------
# 4. LaTeX tables  —  UNCHANGED from original
# ---------------------------------------------------------------------------

def create_latex_tables():
    """
    Create and save two LaTeX tables:
    1. Query performance (min/avg/max + timeout count) per query_set, policy, and triple store
    2. Storage + ingestion metrics per policy and triple store
    """

    # =========================
    # Parameters
    # =========================
    datasets = set(os.environ.get("datasets").split(" "))
    policies = set(os.environ.get("policies").split(" "))
    triplestores = set(os.environ.get("triple_stores").split(" "))
    query_sets = ["lookup", "join", "complex"]

    # =========================
    # Load data
    # =========================
    time_files = [f for f in os.listdir(measurements_in) if f.startswith("time_") and f.endswith(".csv")]
    if not time_files:
        logging.warning("No time_*.csv found — skipping LaTeX table generation.")
        return
    time_path = measurements_in + sorted(time_files)[-1]

    queries_data = pd.read_csv(
        time_path,
        delimiter=";",
        decimal=".",
        dtype={
            "triplestore": "category",
            "dataset": "category",
            "policy": "category",
            "query_set": "category",
            "snapshot": "string",
            "query": "string",
            "execution_time": "float",
            "snapshot_creation_time": "float",
            "yn_timeout": "float",
        },
        parse_dates=["snapshot_ts"],
    )

    query_build_time_data = pd.read_csv(measurements_in + "query_rewriting_times.csv", delimiter=",", decimal=".",
        dtype={
            "dataset": "category",
            "policy": "category",
            "query_set": "category",
            "snapshot": "string",
            "query": "string",
            "rewriting_time": "float"})

    queries_data = queries_data.merge(query_build_time_data, on=["dataset", "policy", "query_set", "snapshot", "query"], how="left")
    queries_data["rewriting_time"] = queries_data["rewriting_time"].fillna(0)
    
    queries_data.loc[queries_data["execution_time"] >= 30, "yn_timeout"] = 1
    queries_data["execution_time_clean"] = queries_data["execution_time"].where(
        (queries_data["execution_time"] <= 30) & (queries_data["execution_time"] >= 0), 
        other=30
    )
    queries_data["execution_time_total"] = (queries_data["execution_time_clean"] + queries_data["rewriting_time"]).clip(upper=30)
    
    ingestion_data = pd.read_csv(measurements_in + "ingestion.csv", delimiter=";", decimal=".")
    ingestion_data["triplestore"] = ingestion_data["triplestore"].str.lower()

    with open(RESULTS_TMPL, "r") as f:
        template_results = f.read()

    queries_agg = queries_data.groupby(["triplestore", "dataset", "policy", "query_set"], observed=False).agg(
        min=("execution_time_total", "min"),
        avg=("execution_time_total", "mean"),
        max=("execution_time_total", "max"),
        cnt_timeout=("yn_timeout", "sum")
    ).reset_index()
    logging.info(f"Aggregated measures:\n{queries_agg}")
    queries_agg = queries_agg[queries_agg["min"].notna()]
    queries_agg.to_csv(f"{os.environ['RUN_DIR']}/output/logs/visualize/queries.csv", index=False)

    storage_agg = ingestion_data.groupby(["triplestore", "dataset", "policy"], observed=False).agg(
        ingestion_time=("ingestion_time", "median"),
        raw_file_size=("raw_file_size_MiB", "mean"),
        db_file_size=("db_files_disk_usage_MiB", "mean")
    ).reset_index()
    storage_agg.to_csv(f"{os.environ['RUN_DIR']}/output/logs/visualize/storage.csv", index=False)

    def format_exec_time(v):
        if v == 0:
            return "0"
        elif v < 0.01:
            return f"{v:.0e}".replace("e-0", "e-").replace("e+0", "e")
        elif v < 1:
            return f"{v:.2f}".lstrip("0")
        else:
            return f"{v:.1f}"

    def format_storage(v):
        return f"{(v / 1024):.2f}"

    placeholder_map = {}
    for dataset in datasets:
        for policy in policies:
            for store in triplestores:
                match_storage = storage_agg[
                    (storage_agg["dataset"] == dataset) &
                    (storage_agg["policy"] == policy) &
                    (storage_agg["triplestore"] == store)
                ]
                if not match_storage.empty:
                    storage_val_raw = format_storage(match_storage["raw_file_size"].values[0])
                    storage_val_db  = format_storage(match_storage["db_file_size"].values[0])
                else:
                    storage_val_raw = "x"
                    storage_val_db  = "x"
                placeholder_map[f"{{{{{dataset}_{store}_{policy}_raw}}}}"]     = storage_val_raw
                placeholder_map[f"{{{{{dataset}_{store}_{policy}_storage}}}}"] = storage_val_db

        for query_set in query_sets:
            for store in triplestores:
                for policy in policies:
                    match_query = queries_agg[
                        (queries_agg["dataset"]     == dataset) &
                        (queries_agg["query_set"]   == query_set) &
                        (queries_agg["triplestore"] == store) &
                        (queries_agg["policy"]      == policy)
                    ]
                    if not match_query.empty:
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_min}}}}"] = format_exec_time(match_query["min"].values[0])
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_avg}}}}"] = format_exec_time(match_query["avg"].values[0])
                        placeholder_map[f"{{{{{dataset}_{store}_{query_set}_{policy}_max}}}}"] = format_exec_time(match_query["max"].values[0])

    filled_table = template_results
    for ph, val in placeholder_map.items():
        filled_table = filled_table.replace(ph, val)

    with open(f"{tables_out}/latex_table_results.tex", "w") as f:
        f.write(filled_table)
    logging.info("LaTeX tables filled and saved.")

    # Evaluation metrics (weighted geometric mean, composite score)
    with open(CONFIG_PATH, "rb") as f:
        config = tomli.load(f)

    version_counts = {ds: cfg["snapshot_versions"] for ds, cfg in config["datasets"].items()}
    version_counts_norm = {k.lower().replace("-", "_"): v for k, v in version_counts.items()}

    def get_version_count(dataset_value):
        key = str(dataset_value).lower().replace("-", "_")
        if key not in version_counts_norm:
            raise KeyError(f"Dataset '{dataset_value}' not in eval_setup.toml. Available: {list(version_counts_norm)}")
        return version_counts_norm[key]

    all_datasets = sorted(queries_agg["dataset"].unique())
    all_stores   = sorted(queries_agg["triplestore"].unique())
    all_policies = sorted(queries_agg["policy"].unique())
    combo_index  = [(s, p) for s in all_stores for p in all_policies]

    ic_ng_sizes = (
        storage_agg[storage_agg["policy"] == "ic_sr_ng"]
        .drop_duplicates(subset=["dataset"])
        .set_index("dataset")["raw_file_size"]
        / 1024.0
    )
    R = np.array([ic_ng_sizes.loc[d] for d in all_datasets if d in ic_ng_sizes.index], dtype=float)
    V = np.array([get_version_count(d) for d in all_datasets if d in ic_ng_sizes.index], dtype=float)
    valid_datasets = [d for d in all_datasets if d in ic_ng_sizes.index]

    w_size      = R / R.sum()
    w_comb_raw  = 0.5 * (R / R.sum()) + 0.5 * (V / V.sum())
    w_comb      = w_comb_raw / w_comb_raw.sum()
    dataset_weight_size = dict(zip(valid_datasets, w_size))
    dataset_weight_comb = dict(zip(valid_datasets, w_comb))

    def weighted_geo_mean_avgs(weight_dict):
        qs_per_dataset = queries_agg.groupby("dataset", observed=False)["query_set"].nunique().to_dict()
        results = {}
        for store, policy in combo_index:
            subset = queries_agg[
                (queries_agg["triplestore"] == store) &
                (queries_agg["policy"] == policy)
            ][["dataset", "query_set", "avg"]].copy()
            subset["row_weight"] = subset["dataset"].map(
                lambda d: weight_dict.get(d, 0.0) / qs_per_dataset.get(d, 1)
            ).astype(float)
            subset = subset.dropna(subset=["avg"])
            if subset.empty or (subset["avg"] <= 0).any():
                results[(store, policy)] = np.nan; continue
            total_w = subset["row_weight"].sum()
            if total_w == 0:
                results[(store, policy)] = np.nan; continue
            subset["row_weight"] /= total_w
            results[(store, policy)] = np.exp((subset["row_weight"] * np.log(subset["avg"])).sum())
        return results

    def weighted_arith_mean_db(weight_dict):
        results = {}
        for store, policy in combo_index:
            total = total_w = 0.0
            for d in valid_datasets:
                m = storage_agg[(storage_agg["triplestore"] == store) & (storage_agg["policy"] == policy) & (storage_agg["dataset"] == d)]
                if m.empty: continue
                total   += weight_dict.get(d, 0.0) * m["db_file_size"].values[0] / 1024.0
                total_w += weight_dict.get(d, 0.0)
            results[(store, policy)] = total / total_w if total_w > 0 else np.nan
        return results

    def minmax_dict(d):
        vals = np.array(list(d.values()), dtype=float)
        vmin, vmax = np.nanmin(vals), np.nanmax(vals)
        if vmax == vmin:
            return {k: 0.0 for k in d}
        return {k: (v - vmin) / (vmax - vmin) for k, v in d.items()}

    geo_avg_A = weighted_geo_mean_avgs(dataset_weight_size)
    geo_avg_B = weighted_geo_mean_avgs(dataset_weight_comb)
    wdb_A     = weighted_arith_mean_db(dataset_weight_size)
    wdb_B     = weighted_arith_mean_db(dataset_weight_comb)
    norm_avg_A, norm_avg_B = minmax_dict(geo_avg_A), minmax_dict(geo_avg_B)
    norm_db_A,  norm_db_B  = minmax_dict(wdb_A),     minmax_dict(wdb_B)
    composite_A = {k: 0.75 * norm_avg_A[k] + 0.25 * norm_db_A[k] for k in combo_index}
    composite_B = {k: 0.75 * norm_avg_B[k] + 0.25 * norm_db_B[k] for k in combo_index}

    def build_metrics_df(weight_dict, geo_avg, weighted_db, norm_avg, norm_db, composite, weight_label):
        rows = []
        for store, policy in combo_index:
            rows.append({
                "triplestore_policy":                         f"{store}-{policy}",
                f"dataset_weight_{weight_label}_BEARB_day":  weight_dict.get("bearb_day",  np.nan),
                f"dataset_weight_{weight_label}_BEARB_hour": weight_dict.get("bearb_hour", np.nan),
                f"dataset_weight_{weight_label}_BEARC":      weight_dict.get("bearc",      np.nan),
                f"dataset_weight_{weight_label}_ORKG":       weight_dict.get("orkg",       np.nan),
                "weighted_geo_mean_avg_s":                   geo_avg.get((store, policy),  np.nan),
                "weighted_arith_mean_db_GiB":                weighted_db.get((store, policy), np.nan),
                "norm_weighted_geo_mean_avg":                 norm_avg.get((store, policy), np.nan),
                "norm_weighted_arith_mean_db":                norm_db.get((store, policy),  np.nan),
                "composite_score_0.75avg_0.25db":             composite.get((store, policy), np.nan),
            })
        return pd.DataFrame(rows).set_index("triplestore_policy")

    m_size     = build_metrics_df(dataset_weight_size, geo_avg_A, wdb_A, norm_avg_A, norm_db_A, composite_A, "size")
    m_size_ver = build_metrics_df(dataset_weight_comb, geo_avg_B, wdb_B, norm_avg_B, norm_db_B, composite_B, "size_and_version")
    m_size     = m_size.dropna(subset=["weighted_geo_mean_avg_s"])
    m_size_ver = m_size_ver.dropna(subset=["weighted_geo_mean_avg_s"])

    m_size.to_csv(f"{tables_out}/metrics_size_weights.csv",               sep=";", decimal=".", float_format="%.6f")
    m_size_ver.to_csv(f"{tables_out}/metrics_size_and_version_weights.csv", sep=";", decimal=".", float_format="%.6f")
    logging.info("Metrics (size weights):\n%s", m_size)
    logging.info("Metrics (size+version weights):\n%s", m_size_ver)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    perf_df   = _load_performance_data()
    ingest_df = _load_ingestion_data()

    combos = list(itertools.product(TRIPLE_STORES, DATASETS))

    for triplestore, dataset in combos:
        plot_query_performance(triplestore, dataset, perf_df,   POLICIES)
        plot_storage(          triplestore, dataset, ingest_df, POLICIES)
        plot_ingest_time(      triplestore, dataset, ingest_df, POLICIES)

    create_latex_tables()