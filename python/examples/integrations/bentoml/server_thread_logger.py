import logging
from typing import Any

import bentoml
import numpy as np
import pandas as pd
from bentoml.io import NumpyNdarray, PandasDataFrame, Text
from numpy.typing import NDArray

import whylogs as why
from whylogs.api.logger.experimental.logger.actor.thread_rolling_logger import (
    ThreadRollingLogger,
)
from whylogs.api.logger.experimental.logger.actor.time_util import (
    Schedule,
    TimeGranularity,
)
from whylogs.api.writer import Writers

logging.basicConfig(level=logging.INFO)

CLASS_NAMES = ["setosa", "versicolor", "virginica"]

iris_clf_runner = bentoml.sklearn.get("iris_knn:latest").to_runner()
svc = bentoml.Service("iris_classifier", runners=[iris_clf_runner])

# Set the WHYLABS_API_KEY environment variable before this
why.init()

ROLLING_LOGGER_KEY_MODEL_1 = "model-1"
ROLLING_LOGGER_KEY_MODEL_2 = "model-2"


@svc.on_startup
def startup(context: bentoml.Context) -> None:
    # Need to create a separate logger for each model
    logger1 = ThreadRollingLogger(
        writers=[Writers.get("whylabs", dataset_id=ROLLING_LOGGER_KEY_MODEL_1)],
        aggregate_by=TimeGranularity.Hour,
        write_schedule=Schedule(cadence=TimeGranularity.Minute, interval=10),
    )

    logger2 = ThreadRollingLogger(
        writers=[Writers.get("whylabs", dataset_id=ROLLING_LOGGER_KEY_MODEL_2)],
        aggregate_by=TimeGranularity.Hour,
        write_schedule=Schedule(cadence=TimeGranularity.Minute, interval=10),
    )

    context.state[ROLLING_LOGGER_KEY_MODEL_1] = logger1
    context.state[ROLLING_LOGGER_KEY_MODEL_2] = logger2

    # Optionally set the log level for the whylogs logger's python logger
    for python_logger_name in logging.Logger.manager.loggerDict:
        python_logger = logging.getLogger(python_logger_name)
        if "ai.whylabs.actor" in python_logger_name:
            python_logger.setLevel(logging.INFO)


@svc.on_shutdown
def shutdown(context: bentoml.Context) -> None:
    logger_1 = context.state[ROLLING_LOGGER_KEY_MODEL_1]
    logger_2 = context.state[ROLLING_LOGGER_KEY_MODEL_2]
    logger_1.close()
    logger_2.close()


@svc.api(
    input=PandasDataFrame(),
    output=PandasDataFrame(),
    route="v1/models/custom_model/predict",
)
def model_2_inference(input_series: pd.DataFrame, context: bentoml.Context) -> pd.DataFrame:
    logger_2: ThreadRollingLogger = context.state[ROLLING_LOGGER_KEY_MODEL_2]
    logger_2.log(input_series)
    # Also perform inference here for another model
    return input_series


@svc.api(
    input=NumpyNdarray.from_sample(np.array([4.9, 3.0, 1.4, 0.2], dtype=np.double)),  # type: ignore
    output=Text(),
    route="v1/models/iris/predict",
)
def classify(features: NDArray[Any], context: bentoml.Context) -> str:
    results = iris_clf_runner.predict.run([features])
    probs = iris_clf_runner.predict_proba.run([features])
    result: int = results[0]

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

    logger_1: ThreadRollingLogger = context.state[ROLLING_LOGGER_KEY_MODEL_1]
    logger_1.log(data)

    return category
