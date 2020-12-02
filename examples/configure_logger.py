"""
Logger options
==============

Example showing the use of a few logger options which can control
output locations
"""
from whylogs.app.session import get_or_create_session
import pandas as pd

# Load some example data, using 'issue_d' as a datetime column
df = pd.read_csv("data/lending_club_1000.csv", parse_dates=["issue_d"])

# Create a whylogs logging session
session = get_or_create_session()
# Log statistics for the dataset with config options
with session.logger(dataset_name="lending-club", dataset_timestamp=df["issue_d"].max(),) as ylog:
    ylog.log_dataframe(df)
    # Note that the logger is active within this context
    print("Logger is active:", ylog.is_active())

# The logger is no longer active
print("Logger is active:", ylog.is_active())
