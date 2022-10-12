from curses import qiflush
import sys
import os
current = os.path.dirname(os.path.realpath(__file__))
parent_directory = os.path.dirname(current)
sys.path.append(parent_directory)
import tzlocal
from datetime import datetime, timedelta, timezone
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

# Version all rows
initial_timestamp = datetime(2022, 10, 12, 14, 43, 21, 941000, timezone(timedelta(hours=2)))
engine.version_all_rows(initial_timestamp)

# Insert
engine.insert([['<http://example.com/Brad_Pitt>', '<http://example.com/occupation>' ,'<http://example.com/Limo_Driver>'],
        ['<http://example.com/Frank_Sinatra>', '<http://example.com/occupation>', '<http://example.com/Singer>']])



# Update
engine.update(
old_triples=[['<http://example.com/Obama>', '<http://example.com/occupation>' ,'<http://example.com/President>'],
             ['<http://example.com/Brad_Pitt>', '<http://example.com/occupation>', '<http://example.com/Limo_Driver>']],
new_triples=[['<http://example.com/Donald_Trump>', None, None],
             [None, None, '<http://example.com/Actor>']])


# Delete

# Query
query = """
PREFIX vers: <https://github.com/GreenfishK/DataCitation/versioning/>

SELECT ?s ?o {
    ?s <http://example.com/occupation> ?o .
}
"""

actual_snapshot = engine.query(query)
print(actual_snapshot)

snapshot_timestamp = initial_timestamp
historical_snapshot = engine.query(query, snapshot_timestamp)
print(historical_snapshot)