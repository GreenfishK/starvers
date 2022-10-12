import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent_directory = os.path.dirname(current)
sys.path.append(parent_directory)

import logging
from src.starvers.starvers import TripleStoreEngine


# Test parameters       
#Home PC
#get_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour"
#post_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour/statements"

#Office
get_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/test"
post_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/test/statements"

LOGGER = logging.getLogger(__name__)
engine = TripleStoreEngine(get_endpoint, post_endpoint)

#Query
#with open("tests/queries/functions__functional_forms_not_exists.txt", "r") as file:
#    query = file.read()
#file.close()
#df = engine.query(query)
#print(df)

# Insert
engine.insert([['<http://example.com/Obama>', '<http://example.com/president_of>' ,'<http://example.com/UnitedStates>'],
        ['<http://example.com/Hamilton>', '<http://example.com/occupation>', '<http://example.com/Formel1Driver>']])



# Update
engine.update(
old_triples=[['<http://example.com/Obama>', '<http://example.com/president_of>' ,'<http://example.com/UnitedStates>'],
             ['<http://example.com/Hamilton>', '<http://example.com/occupation>', '<http://example.com/Formel1Driver>']],
new_triples=[[None, None,'<http://example.com/Canada>'],
             ['<http://example.com/Lewis_Hamilton>', None, None]])

