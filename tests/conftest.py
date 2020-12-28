
import os
import sys

import pytest
_MY_DIR = os.path.realpath(os.path.dirname(__file__))
# Allow import of the test utilities packages
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir, "helpers"))
# Test the parent package
sys.path.insert(0, os.path.join(_MY_DIR, os.pardir,"testdata"))
# Verify whylogs is importable
import whylogs
from whylogs.core.datasetprofile import DatasetProfile

@pytest.fixture(scope="session")
def profile_lending_club():
    import pandas as pd
    import datetime
    from uuid import uuid4 

    now = datetime.datetime.utcnow()
    session_id = uuid4().hex
    df = pd.read_csv(os.path.join(_MY_DIR, os.pardir,"testdata", "lending_club_1000.csv"))
    profile = DatasetProfile(name="test", session_id=session_id, session_timestamp=now)

    profile.track_dataframe(df)

    return profile
