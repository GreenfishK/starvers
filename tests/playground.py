from curses import qiflush
import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent_directory = os.path.dirname(current)
sys.path.append(parent_directory)
import tzlocal
from datetime import datetime, timedelta, timezone
import logging
from src.starvers.starvers import TripleStoreEngine, timestamp_query


# Test parameters       
#Home PC
get_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour"
post_endpoint = "http://192.168.0.52:7200/repositories/BEAR-B_TB_star_h_hour/statements"

#Office
#get_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/test"
#post_endpoint = "http://ThinkPad-T14s-FK:7200/repositories/test/statements"

LOGGER = logging.getLogger(__name__)
engine = TripleStoreEngine(get_endpoint, post_endpoint)

# Query
query = """
Select * {
    <http://identi.ca/user/413169#acct> <http://rdfs.org/sioc/ns#follows> <http://identi.ca/user/31996#acct> .
}
"""

timestamped_query, timestamp = timestamp_query(query)
print(timestamped_query)

