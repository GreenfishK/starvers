from typing import List, Tuple
import pandas as pd
from datetime import datetime


def convert_to_df(nt_text: str) -> pd.DataFrame:
    triples = []
    print(nt_text.splitlines())
    for line in nt_text.splitlines():
        line = line.strip()
        if len(line) == 0:
            continue

        l = line.split(" ", 2)
        triples.append((l[0].strip(" "), l[1].strip(" "), l[2].strip(" .")))

    # Convert to DataFrame
    return pd.DataFrame(triples, columns=["s", "p", "o"])


def convert_df_to_triples(df: pd.DataFrame) -> List[Tuple]:
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