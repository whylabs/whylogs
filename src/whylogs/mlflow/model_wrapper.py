import atexit
import os
import tempfile

import whylogs
import boto3
import datetime


class ModelWrapper(object):
    def __init__(self, model):
        self.model = model
        self.session = whylogs.get_or_create_session()
        self.s3 = boto3.client("s3")
        self.temp_dir = tempfile.gettempdir()
        self.bucket = os.environ['WHYLOGS_S3_BUCKET']
        self.prefix = os.environ['WHYLOGS_S3_PREFIX']
        self.dataset_name = os.environ['WHYLOGS_DATASET_NAME'] or 'live-model'
        self.current_batch_timestamp = _get_utc_hour()
        self.logger = self.create_logger()
        self.last_upload_time = datetime.datetime.utcnow()
        atexit.register(self.upload_whylogs)

    def create_logger(self):
        return self.session.logger(self.dataset_name, dataset_timestamp=self.current_batch_timestamp)

    def predict(self, input):
        now = datetime.datetime.utcnow()
        delta = now - self.last_upload_time
        if delta.total_seconds() > 15:
            self.upload_whylogs()
        current_hour = _get_utc_hour()
        if current_hour > self.current_batch_timestamp:
            # TODO: whylogs to support log rotation. See #106
            self.current_batch_timestamp = current_hour
            self.upload_whylogs()
            self.logger.close()
            self.logger = self.create_logger()
        self.logger.log_dataframe(input)
        return self.model.predict(input)

    def upload_whylogs(self):
        tmp_path = os.path.join(self.temp_dir, 'data.bin')
        self.logger.profile.write_protobuf(tmp_path)
        output = f"{self.prefix}/{self.current_batch_timestamp.timestamp()}-profile.bin"
        self.s3.upload_file(tmp_path, self.bucket, output)
        self.last_upload_time = datetime.datetime.now()


def _get_utc_hour():
    now = datetime.datetime.utcnow()
    return now.replace(minute=0, hour=0, second=0, microsecond=0)
