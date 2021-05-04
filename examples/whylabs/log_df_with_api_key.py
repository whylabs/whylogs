"""
Log a dataframe to WhyLabs
===============

Example of logging a dataframe with log rotation to WhyLabs
"""
import logging
import os
import time

import pandas as pd

from whylogs.app.session import get_or_create_session

os.environ["WHYLABS_API_KEY"] = "<API-KEY>"
os.environ["WHYLABS_API_ENDPOINT"] = "<end point override. not required>"
os.environ["WHYLABS_DEFAULT_ORG_ID"] = "<your-org-id>"
os.environ["WHYLABS_DEFAULT_DATASET_ID"] = "<your-default-dataset-id>"

module_logger = logging.getLogger()
logging.basicConfig(level=logging.DEBUG)

# Load some example data
df = pd.read_csv("../data/lending_club_1000.csv")

# Create a whylogs logging session
session = get_or_create_session()
# Log statistics for the dataset. You can override the dataset id here
with session.logger(tags={"datasetId": "<override-dataset-id>"}, with_rotation_time="1s") as ylog:
    for i in range(1, 10):
        ylog.log_dataframe(df)
        time.sleep(1)
