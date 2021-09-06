from flask.globals import current_app
import numpy as np
from app import df, model
from utils import MessageException
import datetime
import os
from whylogs import get_or_create_session


def modify_random_column_values(value: float = np.random.uniform(low=0.0, high=10.0)) -> None:
    random_column = None
    try:
        number_of_columns = len(df.columns) - 2  # Index and label eliminated
        random_column = df.columns[np.random.randint(number_of_columns) + 1]
        df[random_column] = value
    except Exception as ex:
        raise MessageException(f"Error adding fix value in random column: {str(random_column)}", 500, str(ex))


def add_random_column_outliers(number_outliers: int = 10) -> None:
    random_column = None
    try:
        number_of_columns = len(df.columns) - 2  # Index and label eliminated
        number_of_rows = df.shape[0]
        random_column = df.columns[np.random.randint(number_of_columns) + 1]
        for i in range(number_outliers):
            random_row = np.random.randint(0, number_of_rows)
            df.loc[random_row, random_column] = np.random.uniform(low=20.0, high=50.0)
    except Exception as ex:
        raise MessageException(f"Error adding outliers in random column: {random_column}", 500, str(ex))


def initialize_logger():
    # Initialize session
    whylabs_session = get_or_create_session(os.environ["WHYLABS_CONFIG"])
    n_attemps = int(current_app.config.get("WHYLABS_N_ATTEMPS"))

    while n_attemps > 0:
        # Initialize logger
        whylabs_logger = whylabs_session.logger(
            dataset_name=os.environ["WHYLABS_DEFAULT_DATASET_ID"],
            dataset_timestamp=datetime.datetime.now(datetime.timezone.utc),
            with_rotation_time=os.environ["ROTATION_TIME"]
        )
        if whylabs_logger is not None:
            break
        else:
            n_attemps -= 1
    if n_attemps <= 0:
        raise MessageException("Logger could not be initialized.", 500)
    return whylabs_logger

def get_prediction(data):
    # Convert into nd-array
    try:
        data = np.array(data).reshape(1, -1)
        pred = model.predict(data)[0]
    except Exception as e:
        raise MessageException("Model could not be loaded.", 500, str(e))
    return pred