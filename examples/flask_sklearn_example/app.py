import os
import datetime
import logging
import pandas as pd
import numpy as np
from joblib import load
from flask import Flask, jsonify, request
from whylogs import get_or_create_session
from dotenv import load_dotenv

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# Load environment variables
load_dotenv()
# Initialize session
session = get_or_create_session(".whylabs.yaml")
# logger = session.logger(
#     dataset_name="this_is_my_dataset", 
#     dataset_timestamp=datetime.datetime.now(datetime.timezone.utc), 
#     with_rotation_time="1h"
# )

# Load model with joblib
model = load("model.joblib")

df = pd.read_csv("https://query.data.world/s/4kv7ilu7xzfnwryy6uslb6atoys7lz")

@app.route("/", methods=["GET"])
def health():
    return jsonify({"state": "healthy"})

@app.route("/update", methods=["POST"])
def update_df():
    logger = session.logger(
        dataset_name="this_is_my_dataset", 
        dataset_timestamp=datetime.datetime.now(datetime.timezone.utc), 
        with_rotation_time="1h"
    )
    with logger:
        logger.log_dataframe(df)
    return jsonify({})

@app.route("/predict", methods=["POST"])
def predict():
    # Schema validation


    # Predict the output given the input
    data = request.get_json().get("data")
    # EXCEPTION
    
    n_attemps = 3
    while n_attemps > 0:
        logger = session.logger(
            dataset_name="this_is_my_dataset", 
            dataset_timestamp=datetime.datetime.now(datetime.timezone.utc), 
            with_rotation_time="1h"
        )
        if logger is not None:
            break
        else:
            n_attemps -= 1
    if n_attemps <= 0:
        return jsonify({"Error": "logger error"})
        
    # Convert into nd-array
    data = np.array(data).reshape(1, -1)
    pred = model.predict(data)[0]

    with logger:
        logger.log({"class": pred})

    return jsonify({"class": pred})

if __name__ == "__main__":
    app.run(debug=True)