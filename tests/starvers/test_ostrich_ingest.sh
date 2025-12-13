#!/bin/bash
# This script tests the Ostrich triplestore ingestion and querying functionalities

cd /starvers_eval/databases/ostrich/bearb_day

# Test 1: Query for wikiPageOutDegree of Kane_(wrestler) in two different versions
/opt/ostrich/ostrich-query-version-materialized 0 "http://dbpedia.org/resource/Kane_(wrestler)" "http://dbpedia.org/ontology/wikiPageOutDegree" "?"
# Expected output: 600 and 601 for the object

/opt/ostrich/ostrich-query-version-materialized 1 "http://dbpedia.org/resource/Kane_(wrestler)" "http://dbpedia.org/ontology/wikiPageOutDegree" "?"
# Expected output: 599, 600 and 601 for the object

# Compare the outputs with expected results


# Test 2: Count all triples in all versions and compare them with the expected values from the raw dataset files
