"""
Log a dataframe
===============

Example logging a dataframe.  In this example the config is handled by the
``.whylogs.yaml`` file in the current working directory.

.. literalinclude:: ../../examples/.whylogs.yaml
    :language: yaml

Script
^^^^^^
"""
from whylogs.app.session import get_or_create_session
import pandas as pd

# Load some example data
df = pd.read_csv('data/lending_club_1000.csv')

# Create a WhyLogs logging session
session = get_or_create_session()
# Log statistics for the dataset
with session.logger() as ylog:
    ylog.log_dataframe(df)
