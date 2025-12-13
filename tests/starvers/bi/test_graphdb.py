import requests

REPO = "BI2025"
HOST = "http://localhost:7500"
OUTPUT_FILE = "export.nt"

query = "CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o }"

url = f"{HOST}/repositories/{REPO}"
headers = {
    "Content-Type": "application/sparql-query",
    "Accept": "application/n-triples"
}

print("Exporting repository via CONSTRUCT ...", url)

with requests.post(url, headers=headers, data=query, stream=True) as r:
    print("HTTP status:", r.status_code)
    r.raise_for_status()
    with open(OUTPUT_FILE, "wb") as f:
        for chunk in r.iter_content(chunk_size=8192):
            if chunk:
                f.write(chunk)

print("✔ Exported to", OUTPUT_FILE)
