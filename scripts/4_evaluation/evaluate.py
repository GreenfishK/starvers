from SPARQLWrapper import SPARQLWrapper, POST, DIGEST, GET, JSON

# header: tripleStore,snapshot,min,mean,max,stddev,count,sum
# aggregation on tripleStore and version level

# Run 4 containers in parallel - one for each dataset, each with a different port

# Execute all queries against every snapshot and measure the execution time
# Aggreagte execution time on triple store and snapshot level

# Specific for CB
# Execute queries against repositories starting from v0 (initial snapshot) up until version v x
# Save result sets for every add and del repository
# Initialize final result set
# Iterate over all changeset results until version v
# Add add_result_sets to final set and then remove del_result_sets from final set

# save dataset