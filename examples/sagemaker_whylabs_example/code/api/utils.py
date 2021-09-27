import datetime
import os
from typing import List

import app
import numpy as np
from utils import MessageException


def initialize_logger():
    # Initialize session
    n_attemps = int(os.environ.get("WHYLABS_N_ATTEMPS"))

    while n_attemps > 0:
        # Initialize logger
        app.whylabs_logger = app.whylabs_session.logger(
            dataset_name=os.environ["WHYLABS_DEFAULT_DATASET_ID"],
            dataset_timestamp=datetime.datetime.now(datetime.timezone.utc),
            with_rotation_time=os.environ["ROTATION_TIME"],
        )
        if app.whylabs_logger is not None:
            break
        else:
            n_attemps -= 1
    if n_attemps <= 0:
        raise MessageException("Logger could not be initialized.", 500)


def get_prediction(data: List[float]) -> str:
    # Convert into nd-array
    try:
        data = np.array(data).reshape(1, -1)
        pred = app.model.predict(data)[0]
    except Exception as e:
        raise MessageException("Model could not be loaded.", 500, str(e))
    return pred
