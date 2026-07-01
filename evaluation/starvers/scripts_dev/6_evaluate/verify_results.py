import logging
import os
import sys
import pandas as pd
import tomli
from pathlib import Path

from scripts.logging import setup_logging

# ---------------------------------------------------------------------------
# Logging setup
# ---------------------------------------------------------------------------
BASE_LOG_DIR, LOG = setup_logging("verify_results")


# ---------------------------------------------------------------------------
# Static eval parameters
# ---------------------------------------------------------------------------
CONFIG_PATH = Path("/starvers_eval/configs/eval_setup.toml")
def _load_config() -> dict:
    with open(CONFIG_PATH, "rb") as f:
        return tomli.load(f)
static_eval_params = _load_config()

# ---------------------------------------------------------------------------
# Parameters
# ---------------------------------------------------------------------------
triple_stores = sys.argv[1].split(" ")
policies = sys.argv[2].split(" ")
datasets = sys.argv[3].split(" ")
result_sets_dir = f"{os.environ['RUN_DIR']}/output/result_sets"

result_set_org={dataset: {'snapshots': infos['snapshot_versions'], 'query_sets': list(infos['query_sets'].keys())} 
                for dataset, infos in static_eval_params['datasets'].items()}
df_cnt_rows = pd.DataFrame(columns=['triple_store', 'dataset', 'query_set', 'snapshot', 'result_set', 'policy', 'cnt_rows'])

# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------
for triple_store in triple_stores:         
    for dataset in datasets:
        # Check size of all result sets

        # Count rows and compare the query results resulting from differenc policies for each each dataset. 
        for policy in policies:
            query_sets = result_set_org['dataset']['query_sets']
            cnt_snapshots = result_set_org['dataset']['snapshots']
            for query_set in query_sets:
                for snapshot in range(1, cnt_snapshots + 1):
                    result_set_dir = result_sets_dir + "/" + triple_store.lower() + "/" 
                    + policy + "_" + dataset + "/" + query_set + "/" + str(snapshot)
                    for result_set_file in os.listdir(result_set_dir):
                        LOG.info("Result set absolute path: " + result_set_dir + "/" + result_set_file)
                        result_set = pd.read_csv(result_set_dir + "/" + result_set_file)
                        cnt_rows = len(result_set)
                        df_cnt_rows[len(df_cnt_rows.index)] = [triple_store, dataset, query_set, snapshot,
                                                                result_set, policy, cnt_rows]
                        LOG.info("Cnt rows: " + df_cnt_rows[len(df_cnt_rows.index)])

df_cnt_rows.set_index(['triple_store', 'dataset', 'query_set', 'snapshot', 'result_set', 'policy'], inplace=True)
df_cnt_rows = df_cnt_rows.unstack()

LOG.info(df_cnt_rows.index)
LOG.info(df_cnt_rows)


