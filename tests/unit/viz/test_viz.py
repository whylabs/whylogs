import os
import datetime
from uuid import uuid4

import pandas as pd

from whylogs.core.datasetprofile import DatasetProfile
from whylogs import get_or_create_session

from whylogs.viz import ProfileVisualizer


def test_viz():
    now = datetime.datetime.utcnow()
    session_id = uuid4().hex
    x1 = DatasetProfile(name="test", session_id=session_id, session_timestamp=now, tags={"key": "value"}, metadata={"key": "value"},)
    x1.track("col1", "value")
    viz = ProfileVisualizer()
    viz.available_plots()

    viz.set_profiles([x1])


def test_viz_distribution(df_lending_club):
    session= get_or_create_session()
    with session.logger(dataset_name="another-dataset", dataset_timestamp=datetime.datetime(2020, 9, 22, 0, 0)) as logger:
        logger.log_dataframe(df_lending_club)
    
        viz = ProfileVisualizer()
        viz.set_profiles([logger.profile])
        viz.plot_distribution("loan_amnt")


def test_viz_datatype(df_lending_club):
    session= get_or_create_session()
    with session.logger(dataset_name="another-dataset", dataset_timestamp=datetime.datetime(2020, 9, 22, 0, 0)) as logger:
        logger.log_dataframe(df_lending_club)
    
        viz = ProfileVisualizer()
        viz.set_profiles([logger.profile])

        viz.plot_data_types("emp_length")


def test_viz_uniqueness(df_lending_club):
    session= get_or_create_session()
    with session.logger(dataset_name="another-dataset", dataset_timestamp=datetime.datetime(2020, 9, 22, 0, 0)) as logger:
        logger.log_dataframe(df_lending_club)
    
        viz = ProfileVisualizer()
        viz.set_profiles([logger.profile])

        viz.plot_uniqueness("max_bal_bc")

def test_viz_missing_values(df_lending_club):
    session= get_or_create_session()
    with session.logger(dataset_name="another-dataset", dataset_timestamp=datetime.datetime(2020, 9, 22, 0, 0)) as logger:
        logger.log_dataframe(df_lending_club)
    
        viz = ProfileVisualizer()
        viz.set_profiles([logger.profile])

        viz.plot_missing_values("hardship_payoff_balance_amount")