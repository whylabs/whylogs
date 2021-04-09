"""
Log a series of dataframes grouped by date and send to WhyLabs for visualization
===============

Using data from a Kaggle dataset (https://www.kaggle.com/yugagrawal95/sample-media-spends-data), split
the dataset up by each day using the Calendar_Week column and log each of the data for that day using whylogs.
"""
import pandas as pd
from datetime import datetime
from whylogs.app.session import start_whylabs_session, LoggerKey

csv_file = "data/sample_media_spend.csv"

# Load some sample data
print(f"Loading {csv_file}")
csv_dataframe = pd.read_csv(csv_file)

# Create a WhyLabs logging session
# Note: data collection consent must be explicitly provided since we'll be uploading profiles to WhyLabs.
with start_whylabs_session(data_collection_consent=True) as session:
    # Group each of the rows by the day they occur on using the date string in the Calendar_Week col
    for day_string, dataframe_for_day in csv_dataframe.groupby(['Calendar_Week']):
        # This dataset has dates of the form 9/5/2020
        dt = datetime.strptime(day_string, '%m/%d/%Y')
        print(f"Logging data for {day_string}")

        # whylabs loggers are specific to the dataset's timestamp so we'll be using a different one for each
        # date in our dataset.
        logger = session.logger(args=LoggerKey(dataset_timestamp=dt))

        # log the data to the logger. The logger will write this data out in binary form when it closes, which
        # at the end of the with block in the session's internal logic.
        logger.log_dataframe(dataframe_for_day)
