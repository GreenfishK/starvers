import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent_directory = os.path.dirname(current)
sys.path.append(parent_directory)

import logging
from src.starvers.starvers import TripleStoreEngine

"""
GraphDB 9.3 was used for this test. Below are the endpoints for a local repository. 
The dataset in this repository is an RDF-star variant of the BEAR-B hourly dataset (https://doi.org/10.5281/zenodo.5877503 -> alldata.TB_star_hierarchical.ttl).

"""

# Test parameters 
get_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour"
post_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour/statements"

LOGGER = logging.getLogger(__name__)
engine = TripleStoreEngine(get_endpoint, post_endpoint)

with open("tests/queries/functions__functional_forms_not_exists.txt", "r") as file:
    query = file.read()
file.close()

df = engine.query(query)
#print(df)