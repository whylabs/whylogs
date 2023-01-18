import os

import joblib
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from fastapi.responses import JSONResponse
from pydantic import BaseModel

import whylogs as why

# Set class names for prediction
CLASS_NAMES = ["setosa", "versicolor", "virginica"]

# Load model
model = joblib.load("knn_model.pkl")

# Set WhyLabs variables
os.environ["WHYLABS_DEFAULT_ORG_ID"] = "ORGID"
os.environ["WHYLABS_DEFAULT_DATASET_ID"] = "MODELID"
os.environ["WHYLABS_API_KEY"] = "APIKEY"

app = FastAPI()


# Create Pydantic model for input data
class PredictRequest(BaseModel):
    sepal_length: float
    sepal_width: float
    petal_length: float
    petal_width: float


# Create prediction function
def make_prediction(model, features):

    results = model.predict(features)
    probs = model.predict_proba(features)
    result = results[0]

    output_cls = CLASS_NAMES[result]
    output_proba = max(probs[0])

    return (output_cls, output_proba)


# Create API endpoint
@app.post("/predict")
def predict(request: PredictRequest) -> JSONResponse:

    # Convert input data to list for model prediction
    data = jsonable_encoder(request)
    features = list(data.values())

    # Make prediction with model & add to data dictionary
    predictions = make_prediction(model, [features])
    data["model_class_output"] = predictions[0]
    data["model_prob_output"] = predictions[1]

    # Log input data with whylogs & write to WhyLabs
    profile_results = why.log(data)
    profile_results.writer("whylabs").write()

    return JSONResponse(data)


# Run fast API:
# fastapi robin$ uvicorn api:app --reload
# send values with Swagger UI:
# http://127.0.0.1:8000/docs#/default/predict_predict_post
