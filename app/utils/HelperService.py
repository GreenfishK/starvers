from typing import List, Tuple
import pandas as pd
from datetime import datetime


def convert_df_to_triples(df: pd.DataFrame) -> List[Tuple]:
    result = []
    for index in df.index:
        result.append((df['s'][index], df['p'][index], df['o'][index]))
    return result


def convert_list_to_n3(triples: List[Tuple]) -> List[str]:
    n3: List[str] = []
    for triple in triples:
        n3.append(f"<{triple[0]}> <{triple[1]}> {triple[2]} .")

    return n3

def convert_df_to_n3(df: pd.DataFrame) -> List[str]:
    return [
        f"{row['s']} {row['p']} {row['o']} ." 
        for _, row in df.iterrows()
    ]

def get_timestamp(timestamp: datetime = datetime.now()): 
    return timestamp.strftime("%Y%m%d-%H%M%S") + f"_{timestamp.microsecond // 1000:03d}"