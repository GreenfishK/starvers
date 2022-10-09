import pytest
import logging
from starvers.starvers import TripleStoreEngine

"""
GraphDB 9.3 was used for this test. Below are the endpoints for a local repository. 
The dataset in this repository is an RDF-star variant of the BEAR-B hourly dataset (https://doi.org/10.5281/zenodo.5877503 -> alldata.TB_star_hierarchical.ttl).

"""

# Test parameters 
get_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour"
post_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour/statements"

LOGGER = logging.getLogger(__name__)
engine = TripleStoreEngine(get_endpoint, post_endpoint)

def test_most_basic():
    with open("tests/queries/most_basic.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)

    assert len(df.index) == 43907
 

def test_two_triple_stmts():
    with open("tests/queries/two_triple_stmts.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 4964


def test_functions__functional_forms():
    with open("tests/queries/functions__functional_forms.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1


def test_functions__functional_forms_not_exists():
    with open("tests/queries/functions__functional_forms_not_exists.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1


def test_graph_patterns__aggregate_join():
    with open("tests/queries/graph_patterns__aggregate_join.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1


def test_graph_patterns__bgp():
    with open("tests/queries/graph_patterns__bgp.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1

def test_graph_patterns__extend():
    with open("tests/queries/graph_patterns__extend.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1

def test_graph_patterns__filter():
    with open("tests/queries/graph_patterns__filter.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1

def test_graph_patterns__graph():
    with open("tests/queries/graph_patterns__graph.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1

def test_graph_patterns__group():
    with open("tests/queries/graph_patterns__group.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1

def test_graph_patterns__having():
    with open("tests/queries/graph_patterns__having.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1

def test_graph_patterns__left_join():
    with open("tests/queries/graph_patterns__left_join.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1

def test_graph_patterns__minus():
    with open("tests/queries/graph_patterns__minus.txt", "r") as file:
        query = file.read()
    file.close()
    # TODO: fix approach. Do not restart counting valid_from_x in every BGP but continue counting

    df = engine.query(query)
    assert len(df.index) == 8


def test_graph_patterns__union():
    with open("tests/queries/graph_patterns__union.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 16