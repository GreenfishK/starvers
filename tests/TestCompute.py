#######################################
# Test cases
#######################################
# test 1: entrypoint: ["python", "app/utils/Compute.py", "air_quality_ontology_iterative", "ITERATIVE"] 
# Passed
# Manually executed on 15.08.2025 20:45

# test 2: entrypoint: ["python", "app/utils/Compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_scratch", "20250523-174303_935",
# test 3: entrypoint: ["python", "app/utils/Compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_version", "20250523-174303_935", -> should fail (In the 'from_version' versioning mode, the timestamp needs to be a snaps ...)
# test 4: entrypoint: ["python", "app/utils/Compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_version", "20250523-174303_35", -> should fail (Invalid timestamp format ...)
# test 5 (dependent on test 2 execution): entrypoint: ["python", "app/utils/Compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_version", "20250815-162537_245", -> should work, if 20250815-162537_245.zip was not already processed. Make sure this version is not there when executing test 2
# test 6: entrypoint: ["python", "app/utils/Compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_version", -> should fail (Versioning mode 'from_version' requires ...)
# test 7: entrypoint: ["python", "app/utils/Compute.py", "air_quality_ontology_iterative", "ITERATIVE",  "from_version", "20250523-184303_935", -> should fail(Start timestamp '{start_timestamp}' not found in avai ....)
