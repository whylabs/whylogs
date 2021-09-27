# -*- coding: utf-8 -*-
"""The app module, containing the app factory function."""

import logging
import os

import pandas as pd

# blueprints
from api.views import blueprint
from dotenv import load_dotenv
from flask import Flask, jsonify
from flask_cors import CORS
from joblib import load
from utils import MessageException, message_exception_handler

from whylogs import get_or_create_session

# Load environment variables
load_dotenv()

# Initialize Dataset
df = pd.read_csv(os.environ["DATASET_URL"])

# Load model with joblib
model = load(os.environ["MODEL_PATH"])
whylabs_session = get_or_create_session(os.environ["WHYLABS_CONFIG"])
whylabs_logger = None


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

    register_blueprints(app)
    register_error_handlers(app)

    return app


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
