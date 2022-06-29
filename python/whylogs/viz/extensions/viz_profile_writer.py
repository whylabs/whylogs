import os

import boto3
import pandas as pd
from moto import mock_s3
from moto.s3.responses import DEFAULT_REGION_NAME

import whylogs as why
from whylogs.viz.extensions.reports.summary_drift import SummaryDriftReport

BUCKET_NAME = "my_great_bucket"


def get_report():
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [4.3, 1.8, None, 4.1],
    }
    df = pd.DataFrame(data)
    result_set = why.log(df)
    return SummaryDriftReport(ref_view=result_set.view(), target_view=result_set.view())


def get_s3_resource():
    os.environ["AWS_ACCESS_KEY_ID"] = "my_key_id"
    os.environ["AWS_SECRET_ACCESS_KEY"] = "my_access_key"
    mocks3 = mock_s3()
    mocks3.start()
    resource = boto3.resource("s3", region_name=DEFAULT_REGION_NAME)
    resource.create_bucket(Bucket=BUCKET_NAME)
    return mocks3


mocked_s3 = get_s3_resource()
report = get_report()

# report.writer("local").write()
# report.writer("s3").option(bucket_name=BUCKET_NAME).write()

s3_client = boto3.client("s3")
objects = s3_client.list_objects(Bucket=BUCKET_NAME)
print(objects.get("Contents", []))

mocked_s3.stop()
