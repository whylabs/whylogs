import datetime
from uuid import uuid4

from whylogs.core.datasetprofile import DatasetProfile
from whylogs.viz import ProfileVisualizer

import matplotlib.pyplot as _plt

_plt.ioff()

def test_viz():
    now = datetime.datetime.utcnow()
    session_id = uuid4().hex
    x1 = DatasetProfile(
        name="test",
        session_id=session_id,
        session_timestamp=now,
        tags={"key": "value"},
        metadata={"key": "value"},
    )
    x2 = DatasetProfile(
        name="test",
        session_id=uuid4().hex,
        session_timestamp=now+datetime.timedelta(days=1),
        tags={"key": "value"},
        metadata={"key": "value"},
    )
    x1.track("col1", "value")
    viz = ProfileVisualizer()
    viz.available_plots()

    viz.set_profiles([x1,x2])


def test_viz_distribution(profile_lending_club):

    viz = ProfileVisualizer()
    viz.set_profiles([profile_lending_club])
    viz.plot_distribution("loan_amnt")
    


def test_viz_datatype(profile_lending_club):

    viz = ProfileVisualizer()
    viz.set_profiles([profile_lending_club])

    viz.plot_data_types("emp_length")
    

def test_viz_uniqueness(profile_lending_club):

    viz = ProfileVisualizer()
    viz.set_profiles([profile_lending_club])

    viz.plot_uniqueness("max_bal_bc")
    

def test_viz_string_length(profile_lending_club):

    viz = ProfileVisualizer()
    viz.set_profiles([profile_lending_club])

    viz.plot_string_length("url")


def test_viz_token_length(profile_lending_club):

    viz = ProfileVisualizer()
    viz.set_profiles([profile_lending_club])

    viz.plot_token_length("url",character_list="htp")

def test_viz_char_pos(profile_lending_club):

    viz = ProfileVisualizer()
    viz.set_profiles([profile_lending_club])

    viz.plot_char_pos("url",character_list="htp")
    

def test_viz_missing_values(profile_lending_club):

    viz = ProfileVisualizer()
    viz.set_profiles([profile_lending_club])

    viz.plot_missing_values("hardship_payoff_balance_amount")
    
