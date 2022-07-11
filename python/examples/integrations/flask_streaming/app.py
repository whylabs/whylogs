# -*- coding: utf-8 -*-
"""The app module, containing the app factory function."""

import atexit
import os
import logging

import pandas as pd
from dotenv import load_dotenv # type: ignore
from extensions import init_swagger #type: ignore
from flask import Flask, jsonify, Response
from flask_cors import CORS # type: ignore
from joblib import load # type: ignore
from utils import MessageException, message_exception_handler # type: ignore
from api.views import blueprint  # type: ignore

# Load environment variables
load_dotenv()

# Initialize Dataset
df = pd.read_csv(os.environ["DATASET_URL"])

# Load model with joblib
model = load(os.environ["MODEL_PATH"])
why_logger = None


def create_app(config_object: str = "settings") -> Flask:
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


def register_extensions(app: Flask) -> None:
    """Register Flask extensions."""
    init_swagger(app)
    return None


def register_blueprints(app: Flask) -> None:
    """Register Flask blueprints."""
    # blueprints

    app.register_blueprint(blueprint)
    return None


def register_error_handlers(app: Flask) -> None:
    """Register error handlers."""

    app.register_error_handler(MessageException, message_exception_handler)

    def render_error(error: MessageException) -> Response:
        response = jsonify(error.to_dict())
        response.status_code = error.status_code
        return response


def close_logger_at_exit(app: Flask) -> None:
    app.why_logger.close() # type: ignore
