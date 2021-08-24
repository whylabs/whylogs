import atexit
import datetime
from logging import getLogger
from typing import Union

import numpy as np
import pandas as pd

import whylogs
from whylogs.app.config import WHYLOGS_YML

logger = getLogger(__name__)

PyFuncOutput = Union[pd.DataFrame, pd.Series, np.ndarray, list]


class ModelWrapper(object):
    def __init__(self, model):
        self.model = model
        self.session = whylogs.get_or_create_session("/opt/ml/model/" + WHYLOGS_YML)
        self.ylog = self.create_logger()
        self.last_upload_time = datetime.datetime.utcnow()
        atexit.register(self.ylog.close)

    def create_logger(self):
        # TODO: support different rotation mode and support custom name here
        return self.session.logger("live", with_rotation_time="m")

    def predict(self, data: pd.DataFrame) -> PyFuncOutput:
        """
        Wrapper around https://www.mlflow.org/docs/latest/_modules/mlflow/pyfunc.html#PyFuncModel.predict
        This allows us to capture input and predictions into whylogs
        """
        self.ylog.log_dataframe(data)
        output = self.model.predict(data)

        if isinstance(output, np.ndarray) or isinstance(output, pd.Series):
            data = pd.DataFrame(data=output, columns=["prediction"])
            self.ylog.log_dataframe(data)
        elif isinstance(output, pd.DataFrame):
            self.ylog.log_dataframe(output)
        elif isinstance(output, list):
            for e in output:
                self.ylog.log(feature_name="prediction", value=e)
        else:
            logger.warning("Unsupported output  type: %s", type(output))
        return output
