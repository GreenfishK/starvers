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


