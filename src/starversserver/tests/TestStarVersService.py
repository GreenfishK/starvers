import pandas as pd
import pandas.testing as pdt

from app.utils.HelperService import convert_df_to_n3, convert_to_df

def test_convert_to_df():
    rdf_str ="""
    <http://example.org/city/berlin> <http://www.w3.org/2000/01/rdf-schema#label> "Berlin"@de .
    <http://example.org/city/berlin> <http://example.org/ontology/population> "3769000"^^<http://www.w3.org/2001/XMLSchema#integer> .
"""

    # TODO: fix this because the convert_to_df function does not exist anymore
    df = convert_to_df(rdf_str)

    expected_df = pd.DataFrame({
        "s": ["<http://example.org/city/berlin>", "<http://example.org/city/berlin>"],
        "p": ["<http://www.w3.org/2000/01/rdf-schema#label>", "<http://example.org/ontology/population>"],
        "o": ['"Berlin"@de', '"3769000"^^<http://www.w3.org/2001/XMLSchema#integer>'],
    })

    pdt.assert_frame_equal(df.reset_index(drop=True), expected_df)


def test_get_delta_for_insertions():
    old = pd.DataFrame([("a", "a", "a"), ("b", "b", "b")], columns=["s", "p", "o"])
    new = pd.DataFrame([("a", "a", "a"), ("c", "c", "c")], columns=["s", "p", "o"])

    insertions, _ = __calculate_delta(old, new)
    insertions = convert_df_to_n3(insertions)

    assert len(insertions) == 1
    assert insertions == ["<c> <c> c ."]

def test_get_delta_for_deletions():
    old = pd.DataFrame([("a", "a", "a"), ("b", "b", "b")], columns=["s", "p", "o"])
    new = pd.DataFrame([("a", "a", "a"), ("c", "c", "c")], columns=["s", "p", "o"])

    _, deletions = __calculate_delta(old, new)

    deletions = convert_df_to_n3(deletions)

    assert len(deletions) == 1
    assert deletions == ["<b> <b> b ."]


def __calculate_delta(df1: pd.DataFrame, df2: pd.DataFrame):
        delta = df1.merge(df2, on=["s", "p", "o"], how="outer", indicator=True)

        # Rows only in df1 (deletions)
        deletions = delta[delta['_merge'] == 'left_only'].drop(columns=['_merge'])
        # Rows only in df2 (insertions)
        insertions = delta[delta['_merge'] == 'right_only'].drop(columns=['_merge'])

        return insertions, deletions