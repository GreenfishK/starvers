from typing import List
from datetime import datetime
import requests
import re
import os
from rdflib import Graph
import pandas as pd

from app.exceptions import DatasetNotFoundException
from app.LoggingConfig import get_logger

LOG = get_logger(__name__)


def to_list(nt_text: str) -> List[str]:
    #nt_text = re.sub(r'[\u0000-\u0008\u0009\u000B\u000C\u000E-\u001F\u007F\u00A0\u2028\u2029]', ' ', nt_text)
    # u00011, \u0002\u0003
    # Remove ^^<...#string> only if it appears right before the final dot in a line
    #nt_text = re.sub(
    #    r'(".*?")\^\^<http://www\.w3\.org/2001/XMLSchema#string>(\s*\.)',
    #    r'\1\2',
    #    nt_text
    #)

    lines = nt_text.splitlines()
    clean_lines = [line.strip() for line in lines if line.strip()]
    return clean_lines
    

def convert_df_to_n3(df: pd.DataFrame) -> List[str]:
    return [
        f"{row['s']} {row['p']} {row['o']} ." 
        for _, row in df.iterrows()
    ]


def get_timestamp(timestamp: datetime = datetime.now()) -> str: 
    return timestamp.strftime("%Y%m%d-%H%M%S") + f"_{timestamp.microsecond // 1000:03d}"


def obtain_nt(url: str, output_path: str, chunk_size: int = 65536):
    """
    Downloads an RDF file (.nt or .ttl). If TTL, it converts it to N-Triples before saving.
    """
    # Check supported extensions
    if not (url.endswith(".nt") or url.endswith(".ttl")):
        raise ValueError("Only .nt and .ttl files are supported")

    # Download file
    LOG.info(f"Downloading file from URL: {url}")
    print(f"Downloading file from URL: {url}")
    with requests.get(url, stream=True) as response:
        response.raise_for_status()
        temp_path = output_path + ".tmp"
        total_bytes = 0

        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    total_bytes += len(chunk)

        if total_bytes == 0:
            os.remove(temp_path)
            raise DatasetNotFoundException("Downloaded file is empty")

    # If file is TTL, convert to NT
    if url.endswith(".ttl"):
        LOG.info("Converting .ttl file to .nt file.")
        print("Converting .ttl file to .nt file.")
        try:
            g = Graph()
            g.parse(temp_path, format="turtle")

            LOG.info(f"Saving file to {output_path}")
            print(f"Saving file to {output_path}")
            g.serialize(destination=output_path, format="nt")
        finally:
            os.remove(temp_path)
    else:
        # For .nt, just rename temporary file
        LOG.info(f"Renaming {temp_path} to {output_path}")
        print(f"Renaming {temp_path} to {output_path}")
        os.rename(temp_path, output_path)

    
def normalize_and_skolemize(input_path: str, output_path: str):
    # Escape sequences
    # Chapter 6.4 in https://www.w3.org/TR/turtle/
    # Escape sequences: first representation: \u hex hex hex hex, e.g. \u0008
    escaped_unicode_control_pattern = re.compile(
        r'\\u(?:000[0-9bcefBCEF]|001[0-9a-fA-F]|007f|00a0|2028|2029)'
    )

    # Escape sequences: second representation: as string literals
    # \t, \b, \n, \r, \f, \', \", \\
    # e.g. \u000C is the same as \f
    # \\f escapes "\" and we get \\f -> not a problem

    # Problematic ones:
    # \f (\u0009), \b (\u0008), \ (\u0027)'
    # After a round trip (import and export) these characters are not preserved
    # E.g. \n is preserved after a roundtrip
    problematic_literal_escapes = re.compile(r'(?<!\\)\\[fb\']')

    # 5c 74 (\t): A tab can be mistaken for a space in utf-8 but it is visible in hex
    # GraphDB adds literally "\t" after a roundtrip
    # Replace each occurence with a literal "\t"

    # reserved character escape sequences consist of a '\' followed 
    # by one of ~.-!$&'()*+,;=/?#@%_ and represent the character to the right of the '\'.
    # should not be touched

    # subject and object skolemization pattern
    subject_pattern = re.compile(r'(^_:[a-zA-Z0-9]+)')
    object_pattern = re.compile(
        r'(^[^#].*?\s)(_:[a-zA-Z0-9]+)(\s*(?:<[a-zA-Z0-9_/:.#-]+>)?\s*\.)'
    )
    
    # String datatype pattern
    remove_string_datatype_pattern = re.compile(
        r'(".*?")\^\^<http://www\.w3\.org/2001/XMLSchema#string>(\s*\.)'
    )

    with open(input_path, "r", encoding="utf-8") as infile, \
         open(output_path, "w", encoding="utf-8") as outfile:
        for line in infile:
            if not line.startswith("#"): #dismiss comments
                # Step 1: Normalize — remove control characters and escape sequences
                line = escaped_unicode_control_pattern.sub('', line)
                line = problematic_literal_escapes.sub('', line)

                # Step 1.5: Replace actual tab characters with literal "\t"
                line = line.replace('\t', '\\t')
                
                # Step 2: Normalize — Remove xsd:string datatype
                # This is necessary because upon import into GraphDB, the string datatypes get removed
                line = remove_string_datatype_pattern.sub(r'\1\2', line)

                # Step 3: Skolemize subject and object blank nodes
                line = subject_pattern.sub(r'<\1>', line)
                line = object_pattern.sub(r'\1<\2>\3', line)

                outfile.write(line)
