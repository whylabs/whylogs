import logging
from typing import Any

import bentoml
import numpy as np
import pandas as pd
from bentoml.io import NumpyNdarray, PandasDataFrame, Text
from numpy.typing import NDArray

import whylogs as why
from whylogs.api.logger.experimental.logger.actor.process_rolling_logger import (
    ProcessRollingLogger,
)
from whylogs.api.logger.experimental.logger.actor.time_util import (
    Schedule,
    TimeGranularity,
)

CLASS_NAMES = ["setosa", "versicolor", "virginica"]

iris_clf_runner = bentoml.sklearn.get("iris_knn:latest").to_runner()
svc = bentoml.Service("iris_classifier", runners=[iris_clf_runner])

# Set the WHYLABS_API_KEY environment variable before this
why.init()

ROLLING_LOGGER_KEY = "whylogs_logger"


@svc.on_startup
def startup(context: bentoml.Context) -> None:
    # One instance can log to any number of dataset
    logger = ProcessRollingLogger(
        aggregate_by=TimeGranularity.Hour,
        write_schedule=Schedule(cadence=TimeGranularity.Minute, interval=10),
    )

    # Optionally set the log level for the whylogs logger's python logger
    for python_logger_name in logging.Logger.manager.loggerDict:
        python_logger = logging.getLogger(python_logger_name)
        if "ai.whylabs.actor" in python_logger_name:
            python_logger.setLevel(logging.INFO)

    logger.start()  # Requires manually starting
    context.state[ROLLING_LOGGER_KEY] = logger


@svc.on_shutdown
def shutdown(context: bentoml.Context) -> None:
    logger = context.state[ROLLING_LOGGER_KEY]
    logger.close()


@svc.api(
    input=PandasDataFrame(),
    output=PandasDataFrame(),
    route="v1/models/custom_model/predict",
)
def model_2(input_series: pd.DataFrame, context: bentoml.Context) -> pd.DataFrame:
    logger: ProcessRollingLogger = context.state[ROLLING_LOGGER_KEY]
    logger.log(input_series, dataset_id="model-2")
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

    # numpy values have to be converted to python types  because the process logger
    # requires serializable inputs
    data = {
        "sepal length": features[0].item(),
        "sepal width": features[1].item(),
        "petal length": features[2].item(),
        "petal width": features[3].item(),
        "class_output": category,
        "proba_output": prob.item(),
    }

    logger: ProcessRollingLogger = context.state[ROLLING_LOGGER_KEY]
    logger.log(data, dataset_id="model-1")

    return category
