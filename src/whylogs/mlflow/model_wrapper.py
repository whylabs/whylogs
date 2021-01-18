import atexit

import whylogs
import datetime

from whylogs.app.config import WHYLOGS_YML


class ModelWrapper(object):
    def __init__(self, model):
        self.model = model
        self.session = whylogs.get_or_create_session("/opt/ml/model/" + WHYLOGS_YML)
        self.logger = self.create_logger()
        self.last_upload_time = datetime.datetime.utcnow()
        atexit.register(self.logger.close)

    def create_logger(self):
        # TODO: support different rotation mode and support custom name here
        return self.session.logger('live', with_rotation_time='m')

    def predict(self, df):
        self.logger.log_dataframe(df)
        output = self.model.predict(df)
        self.logger.log_dataframe(df)
        return output
