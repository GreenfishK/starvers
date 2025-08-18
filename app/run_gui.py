from flask import Flask
from app.gui.routes import routes
from app.LoggingConfig import get_logger, setup_logging
import os

logger = get_logger(__name__)

app = Flask(__name__, template_folder='gui/templates', static_folder='gui/static')

# In production, set a strong random value in your environment:
app.secret_key = os.environ['FLASK_SECRET_KEY']

app.register_blueprint(routes)

if __name__ == "__main__":
    setup_logging()
    logger.info("Starting Flask application...")
    app.run(debug=True, port="5000", host="0.0.0.0")


