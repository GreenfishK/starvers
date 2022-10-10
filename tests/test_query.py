import pytest
import logging
from starvers.starvers import TripleStoreEngine


# Test parameters 
#Home PC
#get_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour"
#post_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour/statements"

#Office
get_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/BEAR-B_hourly_TB_star_h"
post_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/BEAR-B_hourly_TB_star_h/statements"

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


def test_functions__functional_forms_not_exists():
    with open("tests/queries/functions__functional_forms_not_exists.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 712


def test_functions__functions_on_dates_and_time():
    with open("tests/queries/functions__functions_on_dates_and_time.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 9

def test_functions__functions_on_numerics():
    with open("tests/queries/functions__functions_on_numerics.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 5

def test_functions__functions_on_rdf_terms():
    with open("tests/queries/functions__functions_on_rdf_terms.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 64
    assert len(df.columns) == 9


def test_functions__functions_on_strings():
    with open("tests/queries/functions__functions_on_strings.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 13


def test_functions__hash_functions():
    with open("tests/queries/functions__hash_functions.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 5


def test_graph_patterns__aggregate_join():
    with open("tests/queries/graph_patterns__aggregate_join.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    sum = df.iloc[0]['sum']
    min = df.iloc[0]['min']
    max = df.iloc[0]['max']
    count = df.iloc[0]['count']

    assert sum == None
    assert min == 'http://dbpedia.org/resource/2015'
    assert max == 'http://dbpedia.org/resource/What_Do_You_Mean%3F'
    assert count == '63 [http://www.w3.org/2001/XMLSchema#integer]'


def test_graph_patterns__bgp():
    with open("tests/queries/graph_patterns__bgp.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 11390
    assert len(df.columns) == 9

def test_graph_patterns__extend():
    with open("tests/queries/graph_patterns__extend.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 226
    assert len(df.columns) == 2

def test_graph_patterns__filter():
    with open("tests/queries/graph_patterns__filter.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    sumPageLength = df.iloc[0]['sumPageLength']
    assert sumPageLength=='3943456 [http://www.w3.org/2001/XMLSchema#integer]'

def test_graph_patterns__graph():
    with open("tests/queries/graph_patterns__graph.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 226
    assert len(df.columns) == 2

def test_graph_patterns__group():
    with open("tests/queries/graph_patterns__group.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 2
    assert len(df.columns) == 6

def test_graph_patterns__having():
    with open("tests/queries/graph_patterns__having.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 2

def test_graph_patterns__join():
    with open("tests/queries/graph_patterns__join.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 672
    assert len(df.columns) == 4

def test_graph_patterns__left_join():
    with open("tests/queries/graph_patterns__left_join.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 8
    assert len(df.columns) == 3

def test_graph_patterns__minus():
    with open("tests/queries/graph_patterns__minus.txt", "r") as file:
        query = file.read()
    file.close()
    # TODO: fix approach with counting valid_from_x in every BGP. 
    # The suffix should be unique for every valid_from_x and valid_from_y in the whole query, not just on BGP or TriplesBlock level.

    df = engine.query(query)
    assert len(df.index) == 8


def test_graph_patterns__union():
    with open("tests/queries/graph_patterns__union.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 16


def test_complex_query_1():
    with open("tests/queries/complex_query_1.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1


def test_complex_query_2():
    with open("tests/queries/complex_query_2.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 720

def test_complex_query_3():
    with open("tests/queries/complex_query_3.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 16