import os
import datetime
import logging
import pandas as pd
from flask import Flask, jsonify, request
from whylogs import get_or_create_session
from dotenv import load_dotenv

app = Flask(__name__)
logging.basicConfig(level=logging.DEBUG)

# Load environment variables
load_dotenv()
# Initialize session
session = get_or_create_session(".whylabs.yaml")
logger = session.logger(
    dataset_name="this_is_my_dataset", 
    dataset_timestamp=datetime.datetime.now(datetime.timezone.utc), 
    with_rotation_time="1h"
)

# Load model with joblib

df = pd.read_csv("https://query.data.world/s/4kv7ilu7xzfnwryy6uslb6atoys7lz")

@app.route("/", methods=["GET"])
def health():
    print(os.system("printenv"))
    return jsonify({"state": "healthy"})

@app.route("/update", methods=["POST"])
def update_df():
    with logger:
        logger.log_dataframe(df)
    return jsonify({})

@app.route("/predict", methods=["POST"])
def predict():
    # Predict the output given the input

    with logger:
        # Log input and output
        logger.log({"confidence": "2"})
        logger.log_dataframe(df)
    return jsonify({"class_id": "exito"})

if __name__ == "__main__":
    app.run(debug=True)