from typing import List, Tuple
import pandas as pd
from datetime import datetime
from SPARQLWrapper import Wrapper
import re
from typing import Union, List, Tuple

from rdflib import Graph

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


def n3_to_set(n3_text: str):
    g = Graph()
    g.parse(data=n3_text, format='nt')  # Assuming N-Triples, not N3
    n3_set = set(g)

    return n3_set

def convert_n3_to_list(nt_text: str) -> List[str]:
    nt_text = re.sub(r'[\u0000-\u0008\u0009\u000B\u000C\u000E-\u001F\u007F\u00A0\u2028\u2029]', ' ', nt_text)
    lines = nt_text.splitlines()
    clean_lines = [line.strip() for line in lines if line.strip()]
    return clean_lines

def convert_n3_to_df_or_list(nt_text: str, output_format: str) -> Union[pd.DataFrame, List[Tuple[str, str, str]]]:
    # Hidden newline breaks (U+2028, U+2029)
    # Invisible formatting characters (U+00A0, non-breaking space)
    # Control characters 
    nt_text = re.sub(r'[\u0000-\u0008\u0009\u000B\u000C\u000E-\u001F\u007F\u00A0\u2028\u2029]', ' ', nt_text)

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

    if output_format == "df":
        # Convert to DataFrame
        return pd.DataFrame(triples, columns=["s", "p", "o"])
    elif output_format == "list":
        return triples
    else:
        raise ValueError(f"Wrong output format: {output_format}. Valid output formats are: df, list")


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