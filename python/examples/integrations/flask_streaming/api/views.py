""" View functions for every endpoint. """
import app  # type: ignore
from flask import Blueprint, request
from flask_pydantic import validate # type: ignore
from schemas import FeatureVector   # type: ignore
from utils import object_response   # type: ignore

from api.utils import get_prediction, initialize_logger  # type: ignore

blueprint = Blueprint("api", __name__, url_prefix="/api/v1")
initialize_logger()


@blueprint.route("/health", methods=["GET"])
def health() -> dict:
    return object_response({"state": "healthy"}, 200)


@blueprint.route("/predict", methods=["POST"])
@validate()
def predict(body: FeatureVector) -> dict:
    # Predict the output given the input vector
    vector = [
        body.sepal_length_cm,
        body.sepal_width_cm,
        body.petal_length_cm,
        body.petal_width_cm,
    ]
    pred = get_prediction(vector)

    # Log input vector as dictionary
    app.why_logger.log(request.json)
    # Log predicted class
    app.why_logger.log({"class": pred})
    return object_response({"class": pred}, 200)
