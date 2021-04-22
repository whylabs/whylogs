"""
Log a dataframe and send the profile to WhyLabs for visualization
===============

Example for logging a dataframe and sending the results to WhyLabs, where the data can be explored further
"""
import pandas as pd

from whylogs.app.session import start_whylabs_session

# Load some sample data
df = pd.read_csv("data/lending_club_1000.csv")

# Create a WhyLabs logging session
# Note: data collection consent must be explicitly provided
# report_progress prints progress bars while uploading profiles. You may want to set it to False in hosted environments
with start_whylabs_session(data_collection_consent=True, report_progress=True) as session:
    # Log statistics for the dataset
    # Resulting dataset profile(s) will be sent to WhyLabs,
    # and you will receive a link to view the pretty charts!
    with session.logger() as logger:
        logger.log_dataframe(df)
