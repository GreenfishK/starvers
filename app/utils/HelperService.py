from typing import List, Tuple
import pandas as pd
from datetime import datetime
from SPARQLWrapper import Wrapper
import re
from typing import Union, List, Tuple

from rdflib import Graph


def convert_n3_to_list(nt_text: str) -> List[str]:
    nt_text = re.sub(r'[\u0000-\u0008\u0009\u000B\u000C\u000E-\u001F\u007F\u00A0\u2028\u2029]', ' ', nt_text)

    # Remove ^^<...#string> only if it appears right before the final dot in a line
    nt_text = re.sub(
        r'(".*?")\^\^<http://www\.w3\.org/2001/XMLSchema#string>(\s*\.)',
        r'\1\2',
        nt_text
    )

    lines = nt_text.splitlines()
    clean_lines = [line.strip() for line in lines if line.strip()]
    
    return clean_lines


def convert_df_to_triples(df: pd.DataFrame) -> List[Tuple]:
    # TODO: Consider removing this method since we are now directly parsing n3 files into lists of triples
    result = []
    for index in df.index:
        result.append((df['s'][index], df['p'][index], df['o'][index]))
    return result


def convert_df_to_n3(df: pd.DataFrame) -> List[str]:
    return [
        f"{row['s']} {row['p']} {row['o']} ." 
        for _, row in df.iterrows()
    ]


def get_timestamp(timestamp: datetime = datetime.now()): 
    return timestamp.strftime("%Y%m%d-%H%M%S") + f"_{timestamp.microsecond // 1000:03d}"