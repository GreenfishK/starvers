from typing import List, Tuple
import pandas as pd
from datetime import datetime
from SPARQLWrapper import Wrapper

def convert_select_query_to_df(result: Wrapper.QueryResult) -> pd.DataFrame:
    """

    :param result:
    :return: Dataframe
    """
    pd.set_option('display.max_columns', None)
    pd.set_option('display.max_colwidth', None)

    def format_value(res_value):
        value = res_value["value"]
        lang = res_value.get("xml:lang")
        datatype = res_value.get("datatype")

        if lang:  # Language-tagged literal
            return f"\"{value}\"@{lang}"
        elif datatype:  # Typed literal
            return f"\"{value}\"^^<{datatype}>"
        else:  # Plain string literal
            return f"<{value}>" if res_value["type"] == "uri" else f"\"{value}\""

    results = result.convert()

    column_names = []
    for var in results["head"]["vars"]:
        column_names.append(var)
    df = pd.DataFrame(columns=column_names)

    values = []
    for r in results["results"]["bindings"]:
        row = []
        for col in results["head"]["vars"]:
            if col in r:
                result_value = format_value(r[col])
            else:
                result_value = None
            row.append(result_value)
        values.append(row)
    df = df.append(pd.DataFrame(values, columns=df.columns))

    return df


def convert_to_df(nt_text: str) -> pd.DataFrame:
    triples = []

    lineNumber = 0
    lines = nt_text.splitlines()

    for line in lines:
        lineNumber += 1

        line = line.strip()
        if len(line) == 0:
            continue

        splitted_line = line.split(" ", 2)            
        if len(splitted_line) != 3:
            raise Exception(f"Failed parsing of line {lineNumber} of {len(lines)}: {line} -> resulted into {splitted_line}")
        
        triples.append((splitted_line[0].strip(" "), splitted_line[1].strip(" "), splitted_line[2].strip(" .")))

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