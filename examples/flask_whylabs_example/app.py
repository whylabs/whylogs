# -*- coding: utf-8 -*-
"""The app module, containing the app factory function."""

import os
import logging
import pandas as pd
from dotenv import load_dotenv
from joblib import load
from flask import Flask, jsonify
from flask_cors import CORS
from utils import MessageException, message_exception_handler
from extensions import init_swagger
from whylogs import get_or_create_session
import atexit
# Load environment variables
load_dotenv()

# Initialize Dataset
df = pd.read_csv(os.environ["DATASET_URL"])

# Load model with joblib
model = load(os.environ["MODEL_PATH"])
whylabs_session = get_or_create_session(os.environ["WHYLABS_CONFIG"])
whylabs_logger = None

# blueprints
from api.views import blueprint


def create_app(config_object="settings"):
    """Create application factory, as explained here: http://flask.pocoo.org/docs/patterns/appfactories/.

    :param config_object: The configuration object to use.
    """

    app = Flask(__name__.split(".")[0])

    # Adding CORS
    CORS(app)

    # Adding Logging
    logging.basicConfig(level=logging.DEBUG)

    app.config.from_object(config_object)

    register_extensions(app)
    register_blueprints(app)
    register_error_handlers(app)
    atexit.register(close_logger_at_exit)

    return app


def register_extensions(app):
    """Register Flask extensions."""
    init_swagger(app)
    return None


def register_blueprints(app):
    """Register Flask blueprints."""
    app.register_blueprint(blueprint)

    return None


def register_error_handlers(app):
    """Register error handlers."""

    app.register_error_handler(MessageException, message_exception_handler)

    def render_error(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response


def close_logger_at_exit():
    whylabs_logger.close()
