import requests
import re

from app.utils.exceptions import DatasetNotFoundException

def download_file(url, output_path, chunk_size=65536):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()  # Raise error for bad status codes

        total_bytes = 0

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # Filter out keep-alive chunks
                    f.write(chunk)
                    total_bytes += len(chunk)

        if total_bytes == 0:
            raise DatasetNotFoundException("Downloaded file is empty")

    
def normalize_and_skolemize(input_path, output_path):
    # Escape sequences
    # Chapter 6.4 in https://www.w3.org/TR/turtle/
    # Escape sequences: first representation: \u hex hex hex hex, e.g. \u0009
    escaped_unicode_control_pattern = re.compile(
        r'\\u(?:000[0-8bcefBCEF]|001[0-9a-fA-F]|007f|00a0|2028|2029)'
    )

    # Escape sequences: second representation: as string literals
    # \t, \b, \n, \r, \f, \', \", \\
    # e.g. \u000C is the same as \f
    # \\f escapes "\" and we get \\f -> not a problem

    # Problematic ones:
    # \f (\u0009), \b (\u0008), \ (\u0027)'
    # After a round trip (import and export) these characters are not preserved
    problematic_literal_escapes = re.compile(r'(?<!\\)\\[fb\']')

    # reserved character escape sequences consist of a '\' followed 
    # by one of ~.-!$&'()*+,;=/?#@%_ and represent the character to the right of the '\'.

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
                
                # Step 2: Normalize — Remove xsd:string datatype
                # This is necessary because upon import into GraphDB, the string datatypes get removed
                line = remove_string_datatype_pattern.sub(r'\1\2', line)

                # Step 3: Skolemize subject and object blank nodes
                line = subject_pattern.sub(r'<\1>', line)
                line = object_pattern.sub(r'\1<\2>\3', line)

                outfile.write(line)

