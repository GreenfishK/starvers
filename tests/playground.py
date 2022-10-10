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
get_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/BEAR-B_hourly_TB_star_h"
post_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/BEAR-B_hourly_TB_star_h/statements"

LOGGER = logging.getLogger(__name__)
engine = TripleStoreEngine(get_endpoint, post_endpoint)

with open("tests/queries/functions__functional_forms_not_exists.txt", "r") as file:
    query = file.read()
file.close()

df = engine.query(query)
#print(df)o



