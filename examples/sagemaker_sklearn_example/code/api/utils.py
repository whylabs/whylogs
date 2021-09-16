from typing import List
import numpy as np
import app
from utils import MessageException
import datetime
import os
from apscheduler.schedulers.background import BackgroundScheduler
import atexit

def modify_random_column_values(value: float = np.random.uniform(low=0.0, high=10.0)) -> None:
    random_column = None
    try:
        number_of_columns = len(app.df.columns) - 2  # Index and label eliminated
        random_column = app.df.columns[np.random.randint(number_of_columns) + 1]
        app.df[random_column] = value
    except Exception as ex:
        raise MessageException(f"Error adding fix value in random column: {str(random_column)}", 500, str(ex))


def add_random_column_outliers(number_outliers: int = 10) -> None:
    random_column = None
    try:
        number_of_columns = len(app.df.columns) - 2  # Index and label eliminated
        number_of_rows = app.df.shape[0]
        random_column = app.df.columns[np.random.randint(number_of_columns) + 1]
        for i in range(number_outliers):
            random_row = np.random.randint(0, number_of_rows)
            app.df.loc[random_row, random_column] = np.random.uniform(low=20.0, high=50.0)
    except Exception as ex:
        raise MessageException(f"Error adding outliers in random column: {random_column}", 500, str(ex))


def initialize_logger():
    # Initialize session
    n_attemps = int(os.environ.get("WHYLABS_N_ATTEMPS"))

    while n_attemps > 0:
        # Initialize logger
        app.whylabs_logger = app.whylabs_session.logger(
            dataset_name=os.environ["WHYLABS_DEFAULT_DATASET_ID"],
            dataset_timestamp=datetime.datetime.now(datetime.timezone.utc),
            with_rotation_time=os.environ["ROTATION_TIME"]
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

def initialized_scheduled_action():
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=modify_random_column_values, trigger="interval", seconds=int(os.environ.get("UPDATE_TIME_IN_SECONDS")))
    scheduler.start()
    atexit.register(lambda: scheduler.shutdown())