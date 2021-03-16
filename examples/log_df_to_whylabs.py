"""
Log a dataframe and send the profile to WhyLabs for visualization
===============

Example for logging a dataframe and sending the results to WhyLabs, where the profile can be explored further
"""
import pandas as pd
from whylogs.app.session import start_whylabs_session

# Load some sample data
df = pd.read_csv("data/lending_club_1000.csv")

# Create a special whylabs logging session
# Note: data collection consent must be explicitly provided
with start_whylabs_session(data_collection_consent=True) as session:
    # Log statistics for the dataset
    # Resulting dataset profile will be sent to WhyLabs,
    # and you will receive a link to view it!
    with session.logger() as ylog:
        ylog.log_dataframe(df)
