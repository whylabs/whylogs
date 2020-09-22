"""
Visualize WhyLogs output
===============

Example with the ProfileVisualizer tool.
In this example we perform a profiling operation and visualize the result using matplotlib

Script
^^^^^^
"""
import datetime

import pandas as pd

from whylogs import get_or_create_session
from whylogs.viz import ProfileVisualizer

session = get_or_create_session()

# Create a list of data profiles
data = pd.read_csv("data/lending_club_demo.csv")
remaining_dates = list(data["issue_d"].unique())[:5]

profiles = []
# list with original profile
for date in remaining_dates:
    subset_data = data[data["issue_d"] == date]
    timestamp = datetime.datetime.strptime(date, "%b-%Y")
    profiles.append(session.log_dataframe(subset_data, dataset_timestamp=timestamp))

# Visualize data
viz = ProfileVisualizer()
viz.set_profiles(profiles)
fig = viz.plot_distribution("annual_inc")
fig.show()
