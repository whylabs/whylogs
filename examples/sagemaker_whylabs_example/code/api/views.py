from flask import Blueprint, request, Response
from flask_pydantic import validate
from api.utils import add_random_column_outliers, initialize_logger, get_prediction
from schemas import FeatureVector
import app
from utils import object_response, message_response

blueprint = Blueprint("api", __name__)
initialize_logger()

@blueprint.route("/ping", methods=["GET"])
def ping():
    """Determine if the container is working and healthy.
    In this sample container, we declare
    it healthy if we can load the model successfully."""
    status = 200
    return Response(response="\n", status=status, mimetype="application/json")

@blueprint.route("/invocations", methods=["POST"])
@validate()
def predict(body: FeatureVector):
    initialize_logger()
    # Predict the output given the input vector
    vector = [
        body.sepal_length_cm,
        body.sepal_width_cm,
        body.petal_length_cm,
        body.petal_width_cm,
    ]
    pred = get_prediction(vector)
    # Log to whylabs platform
    # Log input vector as dictionary and predicted class
    app.whylabs_logger.log(request.json)
    app.whylabs_logger.log({"class": pred})
    app.whylabs_logger.close()
    return object_response({"class": pred}, 200)
