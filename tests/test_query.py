import pytest
import logging
from starvers.starvers import TripleStoreEngine

"""
GraphDB 9.3 was used for this test. Below are the endpoints for a local repository. 
The dataset in this repository is an RDF-star variant of the BEAR-B hourly dataset (https://doi.org/10.5281/zenodo.5877503 -> alldata.TB_star_flat.ttl).

"""

# Test parameters 
get_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_f_hour"
post_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_f_hour/statements"

LOGGER = logging.getLogger(__name__)
engine = TripleStoreEngine(get_endpoint, post_endpoint)

def test_select_all():
    with open("tests/queries/select_all.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)

    assert len(df.index) == 46418
 