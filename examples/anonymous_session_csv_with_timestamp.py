"""
Log a series of dataframes grouped by date and send to WhyLabs for visualization
===============

Using data from a Kaggle dataset (https://www.kaggle.com/yugagrawal95/sample-media-spends-data), split
the dataset up by each day using the Calendar_Week column and log each of the data for that day using whylogs.
"""
from datetime import datetime

import pandas as pd
from tqdm import tqdm

from whylogs.app.session import start_whylabs_session

csv_file = "data/sample_media_spend.csv"

# Load some sample data
print(f"Loading {csv_file}")
csv_dataframe = pd.read_csv(csv_file)
grouped_data = csv_dataframe.groupby(["Calendar_Week"])

# Create a WhyLabs logging session
# Note: data collection consent must be explicitly provided since we'll be uploading profiles to WhyLabs.
# report_progress prints progress bars while uploading profiles. You may want to set it to False in hosted environments
with start_whylabs_session(data_collection_consent=True, report_progress=True) as session, tqdm(grouped_data) as t:
    # Group each of the rows by the day they occur on using the date string in the Calendar_Week col
    for day_string, dataframe_for_day in t:
        # This dataset has dates of the form 9/5/2020
        dt = datetime.strptime(day_string, "%m/%d/%Y")
        t.set_description(f"Logging data for {day_string}")

        # whylabs loggers are specific to the dataset's timestamp so we'll be using a different one for each
        # date in our dataset.
        logger = session.logger(dataset_timestamp=dt)

        # log the data to the logger. The logger will write this data out in binary form when it closes, which
        # at the end of the with block in the session's internal logic.
        logger.log_dataframe(dataframe_for_day)
