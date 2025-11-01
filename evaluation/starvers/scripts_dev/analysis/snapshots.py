import urllib.request, urllib.error, urllib.parse
import json
import os
from pprint import pprint
import csv

def retrieve_snapshot_infos():
    REST_URL = "http://data.bioontology.org"
    API_KEY = "51e656e3-c9bc-477a-8919-40de6d1bf2c1"

    def get_json(url):
        opener = urllib.request.build_opener()
        opener.addheaders = [('Authorization', 'apikey token=' + API_KEY)]
        return json.loads(opener.open(url).read())

    # Get the available resources
    resources = get_json(REST_URL + "/")

    # Get the ontologies from the `ontologies` link
    ontologies = get_json(resources["links"]["ontologies"])

    # Count submissions per ontology
    submission_numbers = []
    cnt_ontologies = len(ontologies)
    for i, ont in enumerate(ontologies):
        print(f"Progress: {i}/{cnt_ontologies}")
        onto_name = ont['name']
        submissions = get_json(ont['links']['submissions'])
        if not submissions:
            submission_numbers.append((onto_name, 0))
        else:
            cnt_submissions = len(submissions)
            submission_numbers.append((onto_name, cnt_submissions))


    # Save submission numbers as CSV
    csv_filename = "ontology_snapshots.csv"
    with open(csv_filename, mode='w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(["onto_name", "cnt_submissions"])
        writer.writerows(submission_numbers)

    print(f"Submission numbers have been saved to {csv_filename}")

def count_dynamic_ontologies():
    # Define the CSV filename
    csv_filename = "ontology_snapshots.csv"

    # Read the CSV file and count ontologies with over 10 snapshots
    snapshot_threshold = 10
    cnt_ontologies_over_threshold = 0

    with open(csv_filename, mode='r', newline='') as file:
        reader = csv.reader(file)
        header = next(reader)  # Skip the header row
        for row in reader:
            onto_name, cnt_submissions = row
            cnt_submissions = int(cnt_submissions)
            if cnt_submissions > snapshot_threshold:
                cnt_ontologies_over_threshold += 1

    print(f"Number of ontologies with over {snapshot_threshold} snapshots: {cnt_ontologies_over_threshold}")

#retrieve_snapshot_infos()
count_dynamic_ontologies()