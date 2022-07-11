# -*- coding: utf-8 -*-
"""The app module, containing the app factory function."""

import atexit
import os
import logging

import pandas as pd
from dotenv import load_dotenv
from extensions import init_swagger
from flask import Flask, jsonify
from flask_cors import CORS
from joblib import load
from utils import MessageException, message_exception_handler

# Load environment variables
load_dotenv()

# Initialize Dataset
df = pd.read_csv(os.environ["DATASET_URL"])

# Load model with joblib
model = load(os.environ["MODEL_PATH"])
why_logger = None

# # blueprints
# from api.views import blueprint


def create_app(config_object="settings"):
    """Create application factory, as explained here: http://flask.pocoo.org/docs/patterns/appfactories/.

    :param config_object: The configuration object to use.
    """

    app = Flask(__name__.split(".")[0])

    # Adding CORS
    CORS(app)

    # Adding Normal Logging
    logging.basicConfig(level=logging.DEBUG)
    
    app.config.from_object(config_object)
    
    register_extensions(app)
    register_blueprints(app)
    register_error_handlers(app)
    atexit.register(close_logger_at_exit, app)
    return app


def register_extensions(app):
    """Register Flask extensions."""
    init_swagger(app)
    return None


def register_blueprints(app):
    """Register Flask blueprints."""
    # blueprints
    from api.views import blueprint

    app.register_blueprint(blueprint)
    return None


def register_error_handlers(app):
    """Register error handlers."""

    app.register_error_handler(MessageException, message_exception_handler)

    def render_error(error):
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response


def close_logger_at_exit(app):
    app.why_logger.close()
