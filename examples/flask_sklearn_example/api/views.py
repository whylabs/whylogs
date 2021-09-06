from flask import Blueprint, current_app
from api.utils import add_random_column_outliers, initialize_logger
from app import df
from utils import object_response, message_response

blueprint = Blueprint("api", __name__, url_prefix="/api/v1")


@blueprint.route("/health", methods=["GET"])
def health():
    return object_response({"state": "healthy"}, 200)


@blueprint.route("/update", methods=["GET"])
def update_df():
    add_random_column_outliers()

    with initialize_logger() as logger:

        logger.log_dataframe(df)

    return message_response("Dataframe Successfully updated", 200)


@blueprint.route("/predict", methods=["GET"])
def predict():
    # Predict the output given the input

    with initialize_logger() as logger:
        # Log input and output
        logger.log({"confidence": "2"})
        logger.log_dataframe(df)
    return object_response({"class_id": "exito"}, 200)