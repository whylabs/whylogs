from flask import Blueprint, request
from flask_pydantic import validate
from api.utils import add_random_column_outliers, initialize_logger, get_prediction
from schemas import FeatureVector
from app import df
from utils import object_response, message_response

blueprint = Blueprint("api", __name__, url_prefix="/api/v1")


@blueprint.route("/health", methods=["GET"])
def health():
    return object_response({"state": "healthy"}, 200)


@blueprint.route("/update", methods=["POST"])
def update_df():
    add_random_column_outliers()
    with initialize_logger() as logger:
        logger.log_dataframe(df)
    return message_response("Dataframe Successfully updated", 200)


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
    with initialize_logger() as logger:
        # Log input vector as dictionary
        logger.log(request.json)
        # Log predicted class
        logger.log({"class": pred})
    return object_response({"class": pred}, 200)
