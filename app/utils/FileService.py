import requests
import re


def download_file(url, output_path, chunk_size=65536):
    with requests.get(url, stream=True) as response:
        response.raise_for_status()  # Raise error for bad status codes

        if len(response.content) == 0:
            raise Exception("File is empty")

        with open(output_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:  # filter out keep-alive chunks
                    f.write(chunk)

    
def skolemize_blank_nodes_in_file(input_path, output_path):
    subject_pattern = re.compile(r'(^_:[a-zA-Z0-9]+)')
    object_pattern = re.compile(
        r'(^[^#].*?\s)(_:[a-zA-Z0-9]+)(\s*(?:<[a-zA-Z0-9_/:.#-]+>)?\s*\.)'
    )

    with open(input_path, "r", encoding="utf-8") as infile, \
         open(output_path, "w", encoding="utf-8") as outfile:
        for line in infile:
            if not line.startswith("#"): #dismiss comments
                line = subject_pattern.sub(r'<\1>', line)
                line = object_pattern.sub(r'\1<\2>\3', line)
                outfile.write(line)