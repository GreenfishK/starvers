import os

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
from matplotlib.lines import Line2D
from matplotlib.patches import Patch
import numpy as np
import pandas as pd
import tomli
from pathlib import Path
from experiments.logging import setup_logging, log_start, log_end
from _svg import load_icon_svg

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
BASE_LOG_DIR, LOG = setup_logging("visualize")

# ---------------------------------------------------------------------------
# Static eval parameters
# ---------------------------------------------------------------------------
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
def _load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)
static_eval_params = _load_config()

# ---------------------------------------------------------------------------
# Environment / path constants
# ---------------------------------------------------------------------------
RUN_DIR          = os.environ["RUN_DIR"]
measurements_in  = f"{RUN_DIR}/output/measurements"
figures_out      = f"{RUN_DIR}/output/figures"
tables_out       = f"{RUN_DIR}/output/tables"

os.makedirs(figures_out, exist_ok=True)
os.makedirs(tables_out,  exist_ok=True)

POLICIES      = os.environ.get("policies", "").split()
DATASETS      = os.environ.get("datasets", "").split()
TRIPLE_STORES = os.environ.get("triple_stores", "").split()

pd.set_option("display.max_columns", None)

# ---------------------------------------------------------------------------
# Colour palette (mirrors the GUI)
# ---------------------------------------------------------------------------
POLICY_COLORS = {
    "ic_sr_ng": "#006699",
    "cb_sr_ng": "#007E71",
    "tb_sr_ng": "#E18922",
    "tb_sr_rs": "#BA4682",
    "tb_sr_re": "#9D9D9C",
    "ostrich": "#5485AB",
    "ostrich_aggchange": "#000000",
}
POLICY_COLOR_FALLBACKS = ["#646363", "#6AAAA5", "#EEB473", "#CD81A8"]

TS_COLORS_CHART = {
    "graphdb": "#006699",
    "jenatdb2": "#E18922",
    "ostrich": "#BA4682",
    "ostrich_aggchange": "#007E71",
}
TS_COLOR_FALLBACKS = ["#000000", "#646363", "#5485AB", "#6AAAA5", "#EEB473"]

DATASET_ORDER = ["bearb_day", "bearb_hour", "bearc", "orkg"]

# Ingest-plot marker shapes: each versioning policy maps to a monochrome SVG
# icon (colour is applied per triple store in the plotting code).
ICON_DIR = Path(__file__).resolve().parent / "templates"
POLICY_ICONS = {
    "tb_sr_ng": "icon_tb_sr_ng.svg",
    "ic_sr_ng": "icon_ic_sr_ng.svg",
    "tb_sr_rs": "icon_tb_sr_rs.svg",
    "tb_sr_re": "icon_tb_sr_re.svg",
    "ostrich": "ostrich_icon.svg",
}


def _policy_marker_path(policy: str):
    """Return the matplotlib Path for a policy's SVG icon (or None)."""
    fname = POLICY_ICONS.get(policy)
    if not fname:
        return None
    path = ICON_DIR / fname
    if not path.exists():
        LOG.warning("Icon not found: %s", path)
        return None
    try:
        return load_icon_svg(path)
    except Exception as exc:  # noqa: BLE001
        LOG.warning("Failed to load icon %s: %s", path, exc)
        return None


def _policy_color_map(policies: list[str]) -> dict[str, str]:
    out = {}
    fallback_idx = 0
    for i, p in enumerate(policies):
        if p in POLICY_COLORS:
            out[p] = POLICY_COLORS[p]
        else:
            out[p] = POLICY_COLOR_FALLBACKS[fallback_idx % len(POLICY_COLOR_FALLBACKS)]
            fallback_idx += 1
    return out


def _policy_label(policy: str) -> str:
    """Human-readable policy name (drops the redundant '_sr' token)."""
    return policy.replace("_sr", "")


def _apply_time_scale(ax, t_min: float, t_max: float) -> None:
    """Format a log y-axis in seconds/minutes, as in the ingestion plot."""
    time_tick_vals = [0.1, 1, 10, 100, 1000, 10000, 100000, 1000000]
    time_tick_labels = ["0.1s", "1s", "10s", "1m 40s", "16m 40s",
                        "2h 46m", "1d 3h", "11d 13h"]
    visible = [(v, l) for v, l in zip(time_tick_vals, time_tick_labels)
               if v >= t_min * 0.2 and v <= t_max * 6]
    if visible:
        ax.set_yticks([v for v, _ in visible])
        ax.set_yticklabels([l for _, l in visible], fontsize=6,
                           fontfamily="monospace")
    ax.yaxis.set_minor_formatter(ticker.NullFormatter())


def _ts_color_map(triplestores: list[str]) -> dict[str, str]:
    out = {}
    fallback_idx = 0
    for i, ts in enumerate(triplestores):
        if ts in TS_COLORS_CHART:
            out[ts] = TS_COLORS_CHART[ts]
        else:
            out[ts] = TS_COLOR_FALLBACKS[fallback_idx % len(TS_COLOR_FALLBACKS)]
            fallback_idx += 1
    return out


# ---------------------------------------------------------------------------
# Data loaders
# ---------------------------------------------------------------------------
def _load_ingestion_data() -> pd.DataFrame:
    """Ingestion measurements, one row per (ts, dataset, policy, run)."""
    path = f"{measurements_in}/storage_and_ingestion.csv"
    if not os.path.exists(path):
        LOG.warning("storage_and_ingestion.csv not found in %s", measurements_in)
        return pd.DataFrame()
    df = pd.read_csv(path, delimiter=";", decimal=".")
    df["triplestore"] = df["triplestore"].str.lower()
    return df


def _load_update_data() -> pd.DataFrame:
    """Update measurements, one row per (run, ts, dataset, policy, batch)."""
    path = f"{measurements_in}/update_time.csv"
    if not os.path.exists(path):
        LOG.warning("update_time.csv not found in %s", measurements_in)
        return pd.DataFrame()
    df = pd.read_csv(path, delimiter=";", decimal=".")
    return df


def _sorted_datasets(available) -> list[str]:
    return [d for d in DATASET_ORDER if d in available] + sorted(
        set(available) - set(DATASET_ORDER)
    )


# ---------------------------------------------------------------------------
# 1. Ingestion chart  —  2 x 2 grid, one plot per dataset
# ---------------------------------------------------------------------------
def create_plot_ingest():
    """
    Style follows the GUI "Ingestion Results — avg over 10 runs".
    A 2x2 grid, one panel per dataset. Each panel is a scatter of DB size
    (GiB, log x-axis) vs avg ingest time (s, log y-axis). Markers encode the
    versioning policy (monochrome SVG icon); the colour of each marker encodes
    the triple store. A vertical line marks the raw dataset size.
    """
    df = _load_ingestion_data()
    if df.empty:
        LOG.warning("No ingestion data — skipping ingest plot.")
        return

    # Average over the repeated runs.
    agg = (
        df.groupby(["triplestore", "dataset", "policy"], as_index=False)
        .agg(
            ingestion_time=("ingestion_time", "mean"),
            raw_file_size_MiB=("raw_file_size_MiB", "mean"),
            db_files_disk_usage_MiB=("db_files_disk_usage_MiB", "mean"),
        )
    )

    datasets = _sorted_datasets(set(agg["dataset"].unique()))
    if not datasets:
        LOG.warning("No datasets in ingestion data — skipping ingest plot.")
        return

    policies = sorted(set(agg["policy"].unique()))
    marker_paths = {p: _policy_marker_path(p) for p in policies}
    ts_colors = _ts_color_map(sorted(set(agg["triplestore"].unique())))

    n_rows = 2
    n_cols = 2
    fig, axes = plt.subplots(
        n_rows, n_cols, figsize=(7.2, 6.4),
        gridspec_kw={"hspace": 0.40, "wspace": 0.3},
    )
    fig.subplots_adjust(left=0.10, right=0.97, top=0.92, bottom=0.28)
    axes = np.atleast_2d(axes).reshape(n_rows, n_cols)

    # Raw-size value per dataset (used only for the vertical line).
    raw_by_ds = {}

    for idx, ds in enumerate(datasets):
        r, c = divmod(idx, n_cols)
        ax = axes[r, c]
        row_df = agg[agg["dataset"] == ds]
        if row_df.empty:
            ax.axis("off")
            ax.set_title(ds, fontsize=9)
            continue

        db_vals_gib = row_df["db_files_disk_usage_MiB"] / 1024
        time_vals = np.maximum(row_df["ingestion_time"], 0.1)

        # ── Scatter points, one per (policy, triplestore).
        #    Marker shape = policy icon; colour = triple store. ──
        for i in range(len(row_df)):
            pol = row_df["policy"].iloc[i]
            ts = row_df["triplestore"].iloc[i]
            col = ts_colors.get(ts, "#666666")
            mpath = marker_paths.get(pol)
            if mpath is None:
                ax.scatter(
                    db_vals_gib.iloc[i], time_vals.iloc[i],
                    s=55, zorder=3, edgecolors="white", linewidths=0.5,
                    color=col,
                )
            else:
                # Icons are 30% smaller than the ostrich silhouette.
                s = 110 if pol == "ostrich" else 110 * 0.49
                ax.scatter(
                    db_vals_gib.iloc[i], time_vals.iloc[i],
                    marker=mpath, s=s, zorder=3,
                    facecolors="none", edgecolors=col, linewidths=1.4,
                )

        # ── Vertical line at raw dataset size ──
        raw_gib = float(row_df["raw_file_size_MiB"].iloc[0]) / 1024
        raw_by_ds[ds] = raw_gib
        ax.axvline(raw_gib, color="#666666", linestyle="--", linewidth=0.8,
                   label="raw size")

        ax.set_xscale("log")
        ax.set_yscale("log")
        ax.set_xlabel("DB Size (GiB)", fontsize=8)
        ax.set_ylabel("Ingest Time", fontsize=8)
        ax.set_title(ds, fontsize=9, fontweight="bold")

        db_min = min(db_vals_gib.min(), raw_gib)
        db_max = max(db_vals_gib.max(), raw_gib)
        ax.set_xlim(max(db_min * 0.3, 1e-6), db_max * 5)
        ax.xaxis.set_major_formatter(ticker.FuncFormatter(
            lambda v, _: f"{v:.2f}" if v < 1 else f"{v:.1f}" if v < 10 else f"{v:.0f}"
        ))
        ax.xaxis.set_minor_formatter(ticker.NullFormatter())

        t_min = max(min(time_vals), 1e-6)
        t_max = max(time_vals)
        ax.set_ylim(t_min * 0.3, t_max * 5)
        _apply_time_scale(ax, t_min, t_max)

        ax.tick_params(axis="x", labelsize=7)
        ax.tick_params(axis="y", labelsize=6)
        ax.spines["top"].set_visible(False)
        ax.spines["right"].set_visible(False)
        ax.grid(True, which="major", alpha=0.2, linewidth=0.5)

        handles, labels_leg = ax.get_legend_handles_labels()
        if handles:
            ax.legend(handles, labels_leg, fontsize=6, framealpha=0.8,
                      loc="best", handletextpad=0.3, borderpad=0.4,
                      labelspacing=0.3)

    # Hide any unused panels
    for idx in range(len(datasets), n_rows * n_cols):
        r, c = divmod(idx, n_cols)
        axes[r, c].axis("off")

    # ── Legend 1: policy icons (monochrome) — marker shape = approach ──
    policy_handles = []
    for pol in policies:
        mpath = marker_paths.get(pol)
        if mpath is not None:
            ms = 9 if pol == "ostrich" else 9 * 0.7
            handle = Line2D([], [], marker=mpath, linestyle="",
                            markerfacecolor="none", markeredgecolor="black",
                            markersize=ms, label=_policy_label(pol))
        else:
            handle = Patch(facecolor="none", edgecolor="black",
                           label=_policy_label(pol))
        policy_handles.append(handle)

    # ── Legend 2: triple-store colours — colour = triplestore ──
    store_handles = [
        Patch(facecolor=ts_colors[ts], edgecolor="black", label=ts)
        for ts in ts_colors
    ]

    fig.suptitle("Ingestion Results — avg over 10 runs",
                 fontsize=11, fontweight="bold")
    fig.legend(handles=policy_handles, loc="lower center", ncol=len(policy_handles),
               fontsize=7, framealpha=0.8, bbox_to_anchor=(0.5, 0.155),
               title="Versioning policy", title_fontsize=6)
    fig.legend(handles=store_handles, loc="lower center", ncol=len(store_handles),
               fontsize=7, framealpha=0.8, bbox_to_anchor=(0.5, 0.105),
               title="Triple store", title_fontsize=6)

    out = f"{figures_out}/ingest.pdf"
    plt.savefig(out, format="pdf")
    plt.close(fig)
    LOG.info("Saved %s", out)


# ---------------------------------------------------------------------------
# 2. Update-performance chart  —  2 x 2 dataset grid, 2 x 2 inside each panel
# ---------------------------------------------------------------------------
def create_plot_updates():
    """
    Style follows the GUI "Update Performance Plots".
    A 2x2 grid, one panel per dataset. Inside each panel a 2x2 grid:
      top-left:    update time for inserted triples
      bottom-left: number of inserted triples
      top-right:   update time for invalidated triples
      bottom-right:number of invalidated triples
    (16 plots total: 4 per panel x 4 panels.)
    """
    df = _load_update_data()
    if df.empty:
        LOG.warning("No update data — skipping update plot.")
        return

    policy_colors = _policy_color_map(sorted(df["policy"].unique()))
    datasets = _sorted_datasets(set(df["dataset"].unique()))

    if not datasets:
        LOG.warning("No datasets in update data — skipping update plot.")
        return

    def classify(row) -> tuple[str, int]:
        batch = str(row["batch"]).strip()
        if batch == "snapshot_0":
            return "insert", 0
        if batch.startswith("positive_change_set"):
            return "insert", int(batch.split("_")[-1])
        if batch.startswith("negative_change_set"):
            return "invalidate", int(batch.split("_")[-1])
        return None, None

    # Representative triplestore per dataset (first available alphabetically).
    ds_ts = {}
    for ds in datasets:
        sub = df[df["dataset"] == ds]["triplestore"].dropna().unique()
        if len(sub):
            ds_ts[ds] = sorted(sub)[0]

    fig = plt.figure(figsize=(12, 10))
    # Outer 2x2 grid of dataset panels.
    outer = fig.add_gridspec(2, 2, hspace=0.35, wspace=0.28,
                             left=0.08, right=0.98, top=0.90, bottom=0.07)

    for idx, ds in enumerate(datasets):
        r, c = divmod(idx, 2)
        ts = ds_ts.get(ds)
        sub = df[(df["dataset"] == ds) & (df["triplestore"] == ts)] if ts else df[df["dataset"] == ds]
        if sub.empty:
            panel = fig.add_subplot(outer[r, c])
            panel.axis("off")
            panel.set_title(ds, fontsize=10, fontweight="bold")
            continue

        inner = sub.copy()
        inner[["batch_type", "version"]] = inner.apply(
            lambda row: pd.Series(classify(row)), axis=1
        )
        inner = inner[inner["batch_type"].notna()]

        # Per (policy, batch_type): map version -> (execution_time, cnt).
        series = {}
        for _, row in inner.iterrows():
            key = (row["policy"], row["batch_type"])
            series.setdefault(key, {})[row["version"]] = (
                row["execution_time"], row["cnt_batch_trpls"]
            )

        versions = sorted(inner["version"].unique())
        v_idx = {v: i for i, v in enumerate(versions)}
        n_versions = len(versions)

        # Inner 2x2 grid inside this panel. A larger hspace gives room so the
        # top row's version-number labels don't collide with the titles of the
        # bottom row (inserted / invalidated triples).
        inner_gs = outer[r, c].subgridspec(2, 2, hspace=0.45, wspace=0.3)

        # TL: update time (insert);  BL: # inserted triples;
        # TR: update time (invalidate);  BR: # invalidated triples.
        plots = {
            (0, 0): ("insert", "time"),
            (1, 0): ("insert", "cnt"),
            (0, 1): ("invalidate", "time"),
            (1, 1): ("invalidate", "cnt"),
        }

        for (ir, ic), (bt, metric) in plots.items():
            sub_ax = fig.add_subplot(inner_gs[ir, ic])
            sub_ax.set_yscale("log")
            sub_ax.set_facecolor("#fafafa")
            sub_ax.grid(True, alpha=0.3)
            sub_ax.tick_params(axis="both", labelsize=6)

            for pol in sorted(policy_colors.keys()):
                ser = series.get((pol, bt))
                if not ser:
                    continue
                pts = [(v_idx[v], vals[0] if metric == "time" else vals[1])
                       for v, vals in sorted(ser.items()) if v in v_idx]
                pts = [(x, y) for x, y in pts if y > 0]
                if not pts:
                    continue
                px = [p[0] for p in pts]
                py = [p[1] for p in pts]
                color = "#000000" if metric == "cnt" else policy_colors[pol]
                sub_ax.plot(px, py, color=color, linewidth=1.8,
                            label=_policy_label(pol) if metric == "time" and ir == 0 else None,
                            solid_capstyle="round")

            label = {"time": "Update time (s)", "cnt": "# triples"}[metric]
            sub_ax.set_ylabel(label, fontsize=7)
            sub_ax.set_xlabel("Version", fontsize=7)
            sub_ax.set_title(f"{bt} {'time' if metric == 'time' else 'triples'}",
                             fontsize=7)

            if metric == "time":
                # Same seconds/minutes scale as the ingestion plot.
                tm = [vals[0] for (spol, sbt), valmap in series.items()
                      if sbt == bt
                      for vals in valmap.values() if vals[0] > 0]
                t_min = max(min(tm), 1e-6) if tm else 1e-3
                t_max = max(tm) if tm else 1
                sub_ax.set_ylim(t_min * 0.3, t_max * 5)
                _apply_time_scale(sub_ax, t_min, t_max)

            n_ticks = min(5, n_versions)
            tick_positions = np.linspace(0, n_versions - 1, n_ticks, dtype=int)
            sub_ax.set_xticks(tick_positions)
            sub_ax.set_xticklabels(
                [versions[i] for i in tick_positions],
                rotation=45, ha="right", fontsize=5,
            )

        # Dataset label above the panel.
        panel_title = ds if not ts else f"{ds}\n({ts})"
        pos = outer[r, c].get_position(fig)
        fig.text(
            (pos.x0 + pos.x1) / 2, pos.y1 + 0.008, panel_title,
            ha="center", va="bottom", fontsize=10, fontweight="bold",
        )

    # Single policy legend from the collected time-line artists.
    if policy_colors:
        from matplotlib.patches import Patch as _Patch
        legend_handles = [_Patch(color=c, label=_policy_label(p))
                          for p, c in policy_colors.items()]
        legend_labels = [_policy_label(p) for p, _ in policy_colors.items()]
        fig.legend(legend_handles, legend_labels, loc="upper left", fontsize=6,
                   ncol=3, framealpha=0.8, bbox_to_anchor=(0.005, 0.96),
                   title="Versioning policy", title_fontsize=6)

    fig.suptitle("Update Performance", fontsize=11, fontweight="bold")
    out = f"{figures_out}/updates.pdf"
    plt.savefig(out, format="pdf", bbox_inches="tight")
    plt.close(fig)
    LOG.info("Saved %s", out)


# ---------------------------------------------------------------------------
# 3. Query-performance LaTeX table (no metric class, no storage/ingestion)
# ---------------------------------------------------------------------------
def create_ltx_table_query_perf():
    """
    Query execution time (min/avg/max + timeout count) per query_set, policy,
    and triple store. No storage / ingestion metrics.
    """
    datasets = set(os.environ.get("datasets").split(" "))
    policies = set(os.environ.get("policies").split(" "))
    triplestores = set(os.environ.get("triple_stores").split(" "))
    query_sets = ["lookup", "join", "complex"]

    time_path = f"{measurements_in}/queries_time.csv"
    if not os.path.exists(time_path):
        LOG.warning("queries_time.csv not found — skipping query perf table.")
        return

    queries_data = pd.read_csv(
        time_path,
        delimiter=";",
        decimal=".",
        dtype={
            "triplestore": "category", "dataset": "category",
            "policy": "category", "query_set": "category",
            "snapshot": "string", "query": "string",
            "execution_time": "float", "snapshot_creation_time": "float",
            "yn_timeout": "float",
        },
        parse_dates=["snapshot_ts"],
    )

    rewrite_path = f"{measurements_in}/queries_rewriting_times.csv"
    if os.path.exists(rewrite_path):
        query_build_time_data = pd.read_csv(
            rewrite_path, delimiter=",", decimal=".",
            dtype={"dataset": "category", "policy": "category",
                   "query_set": "category", "snapshot": "string",
                   "query": "string", "rewriting_time": "float"},
        )
        queries_data = queries_data.merge(
            query_build_time_data,
            on=["dataset", "policy", "query_set", "snapshot", "query"],
            how="left",
        )
        queries_data["rewriting_time"] = queries_data["rewriting_time"].fillna(0)
    else:
        queries_data["rewriting_time"] = 0

    queries_data.loc[queries_data["execution_time"] >= 30, "yn_timeout"] = 1
    queries_data["execution_time_clean"] = queries_data["execution_time"].where(
        (queries_data["execution_time"] <= 30) & (queries_data["execution_time"] >= 0),
        other=30,
    )
    queries_data["execution_time_total"] = (
        queries_data["execution_time_clean"] + queries_data["rewriting_time"]
    ).clip(upper=30)

    queries_agg = queries_data.groupby(
        ["triplestore", "dataset", "policy", "query_set"], observed=False
    ).agg(
        min=("execution_time_total", "min"),
        avg=("execution_time_total", "mean"),
        max=("execution_time_total", "max"),
        cnt_timeout=("yn_timeout", "sum"),
    ).reset_index()
    queries_agg = queries_agg[queries_agg["min"].notna()]
    LOG.info(f"Aggregated measures:\n{queries_agg}")

    def format_exec_time(v):
        if v == 0:
            return "0"
        elif v < 0.01:
            return f"{v:.0e}".replace("e-0", "e-").replace("e+0", "e")
        elif v < 1:
            return f"{v:.2f}".lstrip("0")
        else:
            # Drop a trailing ".0" so 30.0 renders as 30.
            s = f"{v:.1f}"
            return s[:-2] if s.endswith(".0") else s

    # ── Build the table faithfully to the template's multirow layout, but
    #    without the "Metric Class" grouping column and without the storage
    #    (raw/db size) rows.
    #
    #    Column structure (matching the template):
    #      Dataset | Query Set | Metric | <store × policy columns>
    #    Dataset spans all rows of a dataset block (min/avg/max × query sets);
    #    Query Set spans 3 rows (min/avg/max); Metric holds min/avg/max. ──
    present = [s for s in triplestores
               if s in set(queries_agg["triplestore"].unique())]
    if not present:
        LOG.warning("No triple store data in queries_time.csv — skipping.")
        return
    # Deterministic display order (GraphDB, Jena TDB2, Ostrich) as in the paper.
    store_order = ["graphdb", "jenatdb2", "ostrich", "ostrich_aggchange"]
    present_stores = [s for s in store_order if s in present] + \
                     sorted(set(present) - set(store_order))
    if not present_stores:
        LOG.warning("No triple store data in queries_time.csv — skipping.")
        return

    def store_policies(store: str) -> list[str]:
        return sorted(set(queries_agg[
            queries_agg["triplestore"] == store
        ]["policy"].unique()))

    # Number of policy columns per store, total table columns.
    npol_per_store = {s: len(store_policies(s)) for s in present_stores}
    n_data_cols = sum(npol_per_store.values())
    n_col = 3 + n_data_cols

    # Column spec with group separators: Dataset|QuerySet|Metric |store1|store2|...
    col_groups = []
    col_groups.append("l l l ")
    group_intervals = []          # (start, end) column ranges for each store group
    col_index = 4                 # store columns start at column 4
    for s in present_stores:
        n = npol_per_store[s]
        col_groups.append(" ".join(["r"] * n))
        group_intervals.append((col_index, col_index + n - 1))
        col_index += n
    col_spec = "|".join(g.rstrip() for g in col_groups)

    ds_config = static_eval_params.get("datasets", {})

    # Header row 1: dataset / query-set / metric multirows + per-store headers.
    # The last store group has no trailing '|' (matches the column spec).
    hdr1_cells = [
        r"\multirow{6}{*}{Dataset}",
        r"\multirow{6}{*}{\shortstack[c]{Query \\ Set}}",
        r"\multirow{6}{*}{Metric}",
    ]
    for i, s in enumerate(present_stores):
        align = "c" if i == len(present_stores) - 1 else "c|"
        hdr1_cells.append(
            r"\multicolumn{%d}{%s}{\textbf{%s}}" % (npol_per_store[s], align, s)
        )
    hdr1_line = " & ".join(hdr1_cells) + r" \\"

    # Header row 2: rotated policy labels.
    sub_cells = ["", "", ""]
    for s in present_stores:
        for p in store_policies(s):
            sub_cells.append(
                r"\rotatebox{90}{%s}" % _policy_label(p).replace("_", r"\_")
            )
    sub_line = " & ".join(sub_cells) + r" \\"

    # cmidrule below the group headers spanning all store columns (gapless).
    cmid_span = r"\cmidrule{4-%d}" % n_col

    query_sets_ordered = ["lookup", "join", "complex"]
    datasets_present = [d for d in _sorted_datasets(set(queries_agg["dataset"].unique()))
                        if d in datasets]

    body = []
    for dsi, ds in enumerate(datasets_present):
        ds_versions = ds_config.get(ds, {}).get("snapshot_versions")
        if ds == "bearc":
            ds_display = "BEARC"
        elif ds == "orkg":
            ds_display = "ORKG"
        elif ds == "bearb_day":
            ds_display = r"BEARB\_day"
        elif ds == "bearb_hour":
            ds_display = r"BEARB\_hour"
        else:
            ds_display = ds.replace("_", r"\_").upper()
        if ds_versions:
            ds_label = r"\shortstack[c]{%s \\ %s versions}" % (
                ds_display, ds_versions,
            )
        else:
            ds_label = ds_display

        qs_list = [q for q in query_sets_ordered
                   if q in set(queries_agg[queries_agg["dataset"] == ds]["query_set"].unique())]
        n_block_rows = len(qs_list) * 3

        first_qs = True
        for qs in qs_list:
            qs_display = "cplx" if qs == "complex" else qs
            for metric in ["min", "avg", "max"]:
                cells = []
                if first_qs:
                    cells.append(r"\multirow{%d}{*}{%s}" % (n_block_rows, ds_label))
                    first_qs = False
                else:
                    cells.append("")
                cells.append(r"\multirow{3}{*}{%s}" % qs_display if metric == "min" else "")
                cells.append(metric)
                for store in present_stores:
                    for p in store_policies(store):
                        m = queries_agg[
                            (queries_agg["dataset"] == ds) &
                            (queries_agg["query_set"] == qs) &
                            (queries_agg["triplestore"] == store) &
                            (queries_agg["policy"] == p)
                        ]
                        if m.empty:
                            cells.append("x")
                        else:
                            val = format_exec_time(m[metric].values[0])
                            if metric == "avg":
                                to = int(m["cnt_timeout"].values[0])
                                if to > 0:
                                    val = f"{val}({to})"
                            cells.append(val)
                body.append(" & ".join(cells) + r" \\")
            # cmidrule between query-set groups (only if more query sets remain).
            if qs != qs_list[-1]:
                body.append(r"\cmidrule{4-%d}" % n_col)
        if dsi != len(datasets_present) - 1:
            body.append(r"\hline")

    lines = [
        r"\begin{table}[htbp]",
        r"\centering",
        r"\caption{Query execution time (in seconds) for Lookup, Join, and "
        r"complex (cplx) queries across datasets, versioning policies, and "
        r"RDF-star stores. Values are the min, mean, and max over all queries "
        r"and runs. The number of queries that hit the 30-seconds timeout are "
        r"shown in brackets.}",
        r"\label{tab:query_perf}",
        r"\footnotesize",
        r"\setlength{\tabcolsep}{3pt}",
        r"\begin{tabular}{%s}" % col_spec,
        r"\toprule",
        hdr1_line,
        cmid_span,
        sub_line,
        r"\hline",
    ] + body + [
        r"\bottomrule",
        r"\end{tabular}",
        r"\setlength{\tabcolsep}{6pt}",
        r"\end{table}",
        "",
    ]

    out_path = Path(tables_out) / "query_perf.tex"
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    LOG.info("Saved %s", out_path)


# ---------------------------------------------------------------------------
# 4. Dataset-metrics LaTeX table  (renamed from create_dataset_metrics_table)
# ---------------------------------------------------------------------------
def create_ltx_table_ds_metrics():
    """
    Read the dataset metrics CSV produced in the evaluate step and render the
    LaTeX table to tables_out/ds_metrics.tex.
    """
    csv_path = f"{measurements_in}/dataset_metrics.csv"
    if not os.path.exists(csv_path):
        LOG.warning("dataset_metrics.csv not found in %s — skipping ds metrics table.", measurements_in)
        return

    df = pd.read_csv(csv_path, delimiter=",", decimal=".")

    def pct(value) -> str:
        return f"{float(value) * 100:.2f}\\%"

    def int_str(value) -> str:
        return f"{int(value):,}"

    lines = []
    lines.append("\\begin{table}[ht]")
    lines.append("\\centering")
    lines.append("\\caption{Basic dataset infos and BEAR metrics computed from all snapshots and our computed "
                 "changesets. Change ratio, insertion ratio, deletion "
                 "ratio, and growth are reported as mean values over all consecutive versions.}")
    lines.append("\\label{tab:dataset_metrics}")
    lines.append("\\footnotesize")
    lines.append("\\setlength{\\tabcolsep}{3pt}")
    lines.append("\\begin{tabular}{lccccccccc}")
    lines.append("\\toprule")
    lines.append("Dataset & versions & $| V_0 |$ & $| V_{last} |$ & $\\overline{growth}$ & "
                 "$\\bar{\\delta}$ & $\\overline{\\delta^+}$ & $\\overline{\\delta^-}$ & $C_A$ & $O_A$ \\\\")
    lines.append("\\midrule")
    for _, row in df.sort_values("dataset").iterrows():
        tex_dataset = str(row["dataset"]).replace("_", "\\_")
        lines.append(
            f"\\texttt{{{tex_dataset}}} & {int(row['versions'])} & {int_str(row['cnt_triples_first_version'])} "
            f"& {int_str(row['cnt_triples_last_version'])} & {pct(row['mean_growth'])} "
            f"& {pct(row['mean_change_ratio'])} & {pct(row['mean_insertion_ratio'])} "
            f"& {pct(row['mean_deletion_ratio'])} & {int_str(row['cnt_triples_static_core'])} "
            f"& {int_str(row['cnt_triples_version_oblivious'])} \\\\"
        )
    lines.append("\\bottomrule")
    lines.append("\\end{tabular}")
    lines.append("\\setlength{\\tabcolsep}{6pt}")
    lines.append("\\end{table}")

    out_path = Path(tables_out) / "ds_metrics.tex"
    out_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    LOG.info("Saved %s", out_path)


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    log_start("Start Visualize", LOG)
    create_plot_ingest()
    create_plot_updates()
    create_ltx_table_query_perf()
    create_ltx_table_ds_metrics()
    log_end("End Visualize", LOG)
