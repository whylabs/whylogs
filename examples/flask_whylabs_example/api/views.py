""" View functions for every endpoint. """
import app
from api.utils import (
    add_random_column_outliers,
    get_prediction,
    initialize_logger,
    initialized_scheduled_action,
)
from flask import Blueprint, request
from flask_pydantic import validate
from schemas import FeatureVector
from utils import message_response, object_response

blueprint = Blueprint("api", __name__, url_prefix="/api/v1")
initialize_logger()
initialized_scheduled_action()


@blueprint.route("/health", methods=["GET"])
def health():
    return object_response({"state": "healthy"}, 200)


@blueprint.route("/predict", methods=["POST"])
@validate()
def predict(body: FeatureVector):
    # Predict the output given the input vector
    vector = [
        body.sepal_length_cm,
        body.sepal_width_cm,
        body.petal_length_cm,
        body.petal_width_cm,
    ]
    pred = get_prediction(vector)
    # Log to whylabs platform
    # Log input vector as dictionary
    app.whylabs_logger.log(request.json)
    # Log predicted class
    app.whylabs_logger.log({"class": pred})
    return object_response({"class": pred}, 200)
