import os

import bentoml
import numpy as np
from bentoml.io import NumpyNdarray, Text

import whylogs as why

# Set WhyLabs environment variables
os.environ["WHYLABS_API_KEY"] = "ORGID"
os.environ["WHYLABS_DEFAULT_ORG_ID"] = "ORGID"
os.environ["WHYLABS_DEFAULT_DATASET_ID"] = "MODELID"

# Model Prediction Class Names
CLASS_NAMES = ["setosa", "versicolor", "virginica"]

# Load model from BentoML local model store & create a BentoML runner
iris_clf_runner = bentoml.sklearn.get("iris_knn:latest").to_runner()
svc = bentoml.Service("iris_classifier", runners=[iris_clf_runner])


# Create a BentoML API endpoint for model predictions & logging
@svc.api(
    input=NumpyNdarray.from_sample(np.array([4.9, 3.0, 1.4, 0.2], dtype=np.double)),
    output=Text(),
)
async def classify(features: np.ndarray) -> str:
    results = await iris_clf_runner.predict.async_run([features])
    probs = iris_clf_runner.predict_proba.run([features])
    result = results[0]

    category = CLASS_NAMES[result]
    prob = max(probs[0])

    # create a dict of data & prediction results w/ feature names
    data = {
        "sepal length": features[0],
        "sepal width": features[1],
        "petal length": features[2],
        "petal width": features[3],
        "class_output": category,
        "proba_output": prob,
    }

    # Log data+outputs with whylogs & write to WhyLabs.ai
    profile_results = why.log(data)
    profile_results.writer("whylabs").write()

    return category


#  Run with: bentoml serve service:svc --reload
