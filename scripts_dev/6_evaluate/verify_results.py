import logging
import os
import sys
import pandas as pd
import tomli

############################################# Logging #############################################
if not os.path.exists('/starvers_eval/output/logs/evaluate'):
    os.makedirs('/starvers_eval/output/logs/evaluate')
with open('/starvers_eval/output/logs/evaluate/verify_results.txt', "w") as log_file:
    log_file.write("")
logging.basicConfig(handlers=[logging.FileHandler(filename="/starvers_eval/output/logs/evaluate/verify_results.txt", 
                                                  encoding='utf-8', mode='a+')],
                    format="%(asctime)s %(name)s:%(levelname)s:%(message)s", 
                    datefmt="%F %A %T", 
                    level=logging.INFO)

############################################# Parameters #############################################
triple_stores = sys.argv[1].split(" ")
policies = sys.argv[2].split(" ")
datasets = sys.argv[3].split(" ")
result_sets_dir = "/starvers_eval/output/result_sets"
with open("/starvers_eval/configs/eval_setup.toml", mode="rb") as config_file:
    eval_setup = tomli.load(config_file)
result_set_org={dataset: {'snapshots': infos['snapshot_versions'], 'query_sets': list(infos['query_sets'].keys())} 
                for dataset, infos in eval_setup['datasets'].items()}
df_cnt_rows = pd.DataFrame(columns=['triple_store', 'dataset', 'query_set', 'snapshot', 'result_set', 'policy', 'cnt_rows'])

###################################### Verify results ##############################################
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
                        logging.info("Result set absolute path: " + result_set_dir + "/" + result_set_file)
                        result_set = pd.read_csv(result_set_dir + "/" + result_set_file)
                        cnt_rows = len(result_set)
                        df_cnt_rows[len(df_cnt_rows.index)] = [triple_store, dataset, query_set, snapshot,
                                                                result_set, policy, cnt_rows]
                        logging.info("Cnt rows: " + df_cnt_rows[len(df_cnt_rows.index)])

df_cnt_rows.set_index(['triple_store', 'dataset', 'query_set', 'snapshot', 'result_set', 'policy'], inplace=True)
df_cnt_rows = df_cnt_rows.unstack()

logging.info(df_cnt_rows.index)
logging.info(df_cnt_rows)


