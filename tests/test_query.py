from tkinter import E
import pytest
import logging
import SPARQLWrapper
from starvers.starvers import TripleStoreEngine


# Test parameters 
# Endpoints - Home PC
#get_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour"
#post_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour/statements"

# Endpoints - Office
get_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/BEAR-B_hourly_TB_star_h"
post_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/BEAR-B_hourly_TB_star_h/statements"

# Paths
sparql_specs_queries_path = "tests/queries/SPARQL_specs/"
wikidata_queries_path = "tests/queries/wikidata/"

LOGGER = logging.getLogger(__name__)
engine = TripleStoreEngine(get_endpoint, post_endpoint)


def test_complex_query_1():
    with open(sparql_specs_queries_path + "complex_query_1.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1


def test_complex_query_2():
    with open(sparql_specs_queries_path + "complex_query_2.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 720


def test_complex_query_3():
    with open(sparql_specs_queries_path + "complex_query_3.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 16


def test_complex_query_4():
    with open(sparql_specs_queries_path + "complex_query_4.txt", "r") as file:
        query = file.read()
    file.close()
    try:
        df = engine.query(query)
    except Exception as e:
        raise Exception("Malformed query due to mess up with the BINDs. The BINDs in the two sub-BGPs get moved to the " \
        "Select clause in an invalid way.")
  

def test_functions__functional_forms_not_exists():
    with open(sparql_specs_queries_path + "functions__functional_forms_not_exists.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 712


def test_functions__functions_on_dates_and_time():
    with open(sparql_specs_queries_path + "functions__functions_on_dates_and_time.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 9


def test_functions__functions_on_numerics():
    with open(sparql_specs_queries_path + "functions__functions_on_numerics.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 5


def test_functions__functions_on_rdf_terms():
    with open(sparql_specs_queries_path + "functions__functions_on_rdf_terms.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 64
    assert len(df.columns) == 9


def test_functions__functions_on_strings():
    with open(sparql_specs_queries_path + "functions__functions_on_strings.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 13


def test_functions__hash_functions():
    with open(sparql_specs_queries_path + "functions__hash_functions.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 5


def test_graph_patterns__aggregate_join():
    with open(sparql_specs_queries_path + "graph_patterns__aggregate_join.txt", "r") as file:
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


def test_graph_patterns__bgp_single():
    with open(sparql_specs_queries_path + "graph_patterns__bgp_single.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)

    assert len(df.index) == 43907


def test_graph_patterns__bgp():
    with open(sparql_specs_queries_path + "graph_patterns__bgp.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 11390
    assert len(df.columns) == 9

def test_graph_patterns__extend():
    with open(sparql_specs_queries_path + "graph_patterns__extend.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 226
    assert len(df.columns) == 2

def test_graph_patterns__filter():
    with open(sparql_specs_queries_path + "graph_patterns__filter.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    sumPageLength = df.iloc[0]['sumPageLength']
    assert sumPageLength=='3943456 [http://www.w3.org/2001/XMLSchema#integer]'

def test_graph_patterns__graph():
    with open(sparql_specs_queries_path + "graph_patterns__graph.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 226
    assert len(df.columns) == 2

def test_graph_patterns__group():
    with open(sparql_specs_queries_path + "graph_patterns__group.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 2
    assert len(df.columns) == 6

def test_graph_patterns__having():
    with open(sparql_specs_queries_path + "graph_patterns__having.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1
    assert len(df.columns) == 2

def test_graph_patterns__join():
    with open(sparql_specs_queries_path + "graph_patterns__join.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 672
    assert len(df.columns) == 4

def test_graph_patterns__left_join():
    with open(sparql_specs_queries_path + "graph_patterns__left_join.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 8
    assert len(df.columns) == 3

def test_graph_patterns__minus():
    with open(sparql_specs_queries_path + "graph_patterns__minus.txt", "r") as file:
        query = file.read()
    file.close()
    
    df = engine.query(query)
    assert len(df.index) == 8


def test_graph_patterns__no_vars():
    with open(sparql_specs_queries_path + "graph_patterns__no_vars.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1
    # TODO: Implement ask queries in algebra.translateAlgebra
    # TODO: Implement this test.


def test_graph_patterns__union():
    with open(sparql_specs_queries_path + "graph_patterns__union.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 16


def test_operators__arithmetics():
    with open(sparql_specs_queries_path + "operators__arithmetics.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    test_arithmetics = df.iloc[0]['test_arithmetics']
    assert test_arithmetics=='6 [http://www.w3.org/2001/XMLSchema#integer]'

def test_operators__conditional_and():
    with open(sparql_specs_queries_path + "operators__conditional_and.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    test_arithmetics = df.iloc[0]['test_arithmetics']
    assert test_arithmetics=='6 [http://www.w3.org/2001/XMLSchema#integer]'

def test_operators__conditional_or():
    with open(sparql_specs_queries_path + "operators__conditional_or.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    test_arithmetics = df.iloc[0]['test_arithmetics']
    assert test_arithmetics=='6 [http://www.w3.org/2001/XMLSchema#integer]'

def test_operators__relational():
    with open(sparql_specs_queries_path + "operators__relational.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    test_arithmetics = df.iloc[0]['test_arithmetics']
    assert test_arithmetics=='6 [http://www.w3.org/2001/XMLSchema#integer]'           


def test_operators__unary():
    with open(sparql_specs_queries_path + "operators__unary.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    test_arithmetics = df.iloc[0]['test_arithmetics']
    assert test_arithmetics=='6 [http://www.w3.org/2001/XMLSchema#integer]' 


def test_other__service_and_triple():
    with open(sparql_specs_queries_path + "other__service_and_triple.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 84
    assert len(df.columns) == 4


def test_other__service_empty():
    with open(sparql_specs_queries_path + "other__service_empty.txt", "r") as file:
        query = file.read()
    file.close()

    try:
        df = engine.query(query)
    except TypeError as e:
        raise Exception("Error in algebra.translateAlgebra(query_algebra):" \
            " As the SERVICE block is empty there is no pattern in node.part and iterations over NoneType throws an error")


def test_other__service_nested():
    with open(sparql_specs_queries_path + "other__service_nested.txt", "r") as file:
        query = file.read()
    file.close()

    try:
        df = engine.query(query)
    except RecursionError as e:
        raise Exception("Error in parser.parseQuery function: Maximum recursion reached.")
    assert 1==1


def test_other__service_simple():
    with open(sparql_specs_queries_path + "other__service_simple.txt", "r") as file:
        query = file.read()
    file.close()

    try:
        df = engine.query(query)
    except Exception as e:
        # Somehow the raised exception is not recognized as exception.
        raise("Something went wrong in algebra.translateAlgebra. It did not produce a valid query.")
     

def test_other__values():
    with open(sparql_specs_queries_path + "other__values.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 1663
    assert len(df.columns) == 3


def test_property_path__alternative_path():
    with open(sparql_specs_queries_path + "property_path__alternative_path.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 31
    assert len(df.columns) == 4
    # TODO: implement functionality in starvers.starvers.timestamp_query


def test_property_path__sequence_path():
    with open(sparql_specs_queries_path + "property_path__sequence_path.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 14
    assert len(df.columns) == 2
    

def test_property_path__inverse_path():
    with open(sparql_specs_queries_path + "property_path__inverse_path.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 48
    assert len(df.columns) == 2


def test_property_path__negated_property_set():
    with open(sparql_specs_queries_path + "property_path__negated_property_set.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 87700
    # TODO: implement functionality in starvers.starvers.timestamp_query


def test_property_path__one_or_more():
    with open(sparql_specs_queries_path + "property_path__one_or_more.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1
    # TODO: implement functionality in starvers.starvers.timestamp_query
    # TODO: implement test


def test_property_path__zero_or_more():
    with open(sparql_specs_queries_path + "property_path__zero_or_more.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1
    # TODO: implement functionality in starvers.starvers.timestamp_query
    # TODO: implement test


def test_property_path__zero_or_one():
    with open(sparql_specs_queries_path + "property_path__zero_or_one.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert 1 == 1
    # TODO: implement functionality in starvers.starvers.timestamp_query
    # TODO: implement test


def test_solution_modifiers__distinct():
    with open(sparql_specs_queries_path + "solution_modifiers__distinct.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 63


def test_solution_modifiers__order_by():
    with open(sparql_specs_queries_path + "solution_modifiers__order_by.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 63
    # TODO: fix the insertion of DESC and ASC in the order by clause


def test_solution_modifiers__reduced():
    with open(sparql_specs_queries_path + "solution_modifiers__reduced.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 63


def test_solution_modifiers__slice():
    with open(sparql_specs_queries_path + "solution_modifiers__slice.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 50 


def test_solution_modifiers__to_multiset():
    with open(sparql_specs_queries_path + "solution_modifiers__to_multiset.txt", "r") as file:
        query = file.read()
    file.close()

    df = engine.query(query)
    assert len(df.index) == 82
