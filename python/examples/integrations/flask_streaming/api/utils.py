import os
from typing import List

import app  # type: ignore
import numpy as np

from utils import MessageException # type: ignore

import whylogs as why


def initialize_logger() -> None:
    # Initialize session
    n_attempts = 3
    while n_attempts > 0:
        # Initialize logger
        app.why_logger = why.logger(mode="rolling", interval=5, when="M", base_name="whylogs-flask-example") # type: ignore
        if os.environ["WHYLABS_DEFAULT_DATASET_ID"]:
            app.why_logger.append_writer("whylabs")
        else:
            app.why_logger.append_writer("local", base_dir="logs")

        if app.why_logger is not None:
            break
        else:
            n_attempts -= 1
    if n_attempts <= 0:
        raise MessageException("Logger could not be initialized.", 500)


def get_prediction(data: List[float]) -> str:
    # Convert into nd-array
    try:
        data = np.array(data).reshape(1, -1) # type: ignore
        pred = app.model.predict(data)[0]
    except Exception as e:
        raise MessageException("Model could not be loaded.", 500, str(e))
    return pred
