
from whylogs.core.datasetprofile import DatasetProfile
import whylogs
import os
import sys
import pandas as pd

import pytest
_MY_DIR = os.path.realpath(os.path.dirname(__file__))
# Allow import of the test utilities packages
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir, "helpers"))
# Test the parent package
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir, "testdata"))
# Verify whylogs is importable


@pytest.fixture(scope="session")
def profile_lending_club():

    import datetime
    from uuid import uuid4

    now = datetime.datetime.utcnow()
    session_id = uuid4().hex
    df = pd.read_csv(os.path.join(_MY_DIR, os.pardir,
                                  "testdata", "lending_club_1000.csv"))
    profile = DatasetProfile(
        name="test", session_id=session_id, session_timestamp=now)

    profile.track_dataframe(df)

    return profile


@pytest.fixture(scope="session")
def s3_config_path():
    config_path = os.path.join(
        _MY_DIR, os.pardir, "testdata", ".whylogs_s3.yaml")
    return config_path


@pytest.fixture(scope="session")
def df_lending_club():

    df = pd.read_csv(os.path.join(_MY_DIR, os.pardir,
                                  "testdata", "lending_club_1000.csv"))

    return df.head(50)
