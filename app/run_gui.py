from flask import Flask
from app.gui.routes import routes
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__, template_folder='gui/templates', static_folder='gui/static')
app.register_blueprint(routes)

if __name__ == "__main__":
    logging.info("Starting Flask application...")
    app.run(debug=True, port="5000", host="0.0.0.0")


# TODO:
# Add following infos
# - Polling data:
#   - polling_interval: available in the postgres table dataset.polling_interval
#   - next polling timestamp: should be computed from the dataset.polling_interval and the last timestamp
#   in f"/starvers/evaluation/{repo_name}/{repo_name}_timings.csv" in column 1
#   - rdf_dataset_url: available in the postgres table dataset.rdf_dataset_url
# The following SQL statement can be used to get the polling intervall and rdf_dataset_url for a specific repository:
# select repository_name, rdf_dataset_url, polling_interval from public.dataset where repository_name = '<repo_name>';
# - A plot with the number of absolute triples in the respective versions.
# It should be next to the Deltas plot