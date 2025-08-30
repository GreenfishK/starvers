#######################################
# Test cases
#######################################
# test 1: entrypoint: ["python", "app/utils/compute.py", "air_quality_ontology_iterative", "ITERATIVE"] 
# Passed
# Manually executed on 15.08.2025 20:45

# test 2: entrypoint: ["python", "app/utils/compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_scratch", "20250523-174303_935"]
# Passed.
# Manually executed on 15.08.2025 20:57

# test 3: entrypoint: ["python", "app/utils/compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_version", "20250523-174303_935"] -> should fail (In the 'from_version' versioning mode, the timestamp needs to be a snaps ...)
# Passed. 
# Manually executed on 15.08.2025 21:19

# test 4: entrypoint: ["python", "app/utils/compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_version", "20250523-174303_35", -> should fail (Invalid timestamp format ...)
# Passed. 
# Manually executed on 16.08.2025 20:14

# test 5: entrypoint: ["python", "app/utils/compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_version", "20250803-095301_343"] -> should work
# dependencies: all snapshots until including version 20250730-133611_505 must have been processed
# Passed
# Manually executed on 16.08.2025 20:49

# test 6: entrypoint: ["python", "app/utils/compute.py", "air_quality_ontology_iterative", "ITERATIVE", "from_version"] -> should fail (Versioning mode 'from_version' requires ...)
# Passed
# Manually executed on 16.08.2025 20:57

# test 7: entrypoint: ["python", "app/utils/compute.py", "air_quality_ontology_iterative", "ITERATIVE", "from_version", "20250523-184303_935"] -> should fail(Start timestamp '{start_timestamp}' not found in avai ....)
# Passed
# Manually executed on 16.08.2025 20:58