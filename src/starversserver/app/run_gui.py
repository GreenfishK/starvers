from app.gui.routes import routes
from app.LoggingConfig import get_logger, setup_logging
import os

from flask import Flask
from werkzeug.middleware.proxy_fix import ProxyFix


logger = get_logger(__name__)

# Resolve paths relative to this file so they are correct regardless of
# the working directory the process is launched from.
_here = os.path.dirname(os.path.abspath(__file__))
 
app = Flask(
    __name__,
    template_folder=os.path.join(_here, 'gui', 'templates'),
    static_folder=os.path.join(_here, 'gui', 'static'),
)
 

# Trust one layer of reverse-proxy headers (nginx)
app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_port=1, x_prefix=1)

# In production, set a strong random value in your environment:
app.secret_key = os.environ['FLASK_SECRET_KEY']

app.register_blueprint(routes)

if __name__ == "__main__":
    setup_logging()
    logger.info("Starting Flask application...")
    app.run(debug=True, host="0.0.0.0", port="5000")