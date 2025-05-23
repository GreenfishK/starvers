from flask import Flask
from app.routes import routes
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

app = Flask(__name__, template_folder='app/templates', static_folder='app/static')
app.register_blueprint(routes)

if __name__ == "__main__":
    logging.info("Starting Flask application...")
    app.run(debug=True, port="5000", host="0.0.0.0")


# TODO:
# Add following elements to the GUI
# - A plot that shows the number of added and deleted triples over time for the selected repository. 
# The input files can be found in /starvers/evaluation/{repo_name}/{repo_name}_timing.csv
# The timestamp is in the first column, the number of added triples in the second column and the number of deleted triples in the third column.
# The plot should be added under the Dropdown menu for the repo selection.
# - Range of accessible timestamps for the selected repository. Display the range above the timestamp field as an info in the following format: from: start_timestamp to: end_timestamp
# Both can be extracted from the first column in /starvers/evaluation/{repo_name}/{repo_name}_timing.csv
# - Make the timestamp field smaller. Big enough to fit the timestamp, but not too big.