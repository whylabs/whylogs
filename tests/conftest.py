import os
import sys

import pandas as pd
import pytest

from whylogs.core.datasetprofile import DatasetProfile

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

    now = datetime.datetime.now(datetime.timezone.utc)
    session_id = uuid4().hex
    df = pd.read_csv(os.path.join(_MY_DIR, os.pardir, "testdata", "lending_club_1000.csv"))
    profile = DatasetProfile(name="test", session_id=session_id, session_timestamp=now)

    profile.track_dataframe(df)

    return profile


@pytest.fixture(scope="session")
def s3_config_path():
    config_path = os.path.join(_MY_DIR, os.pardir, "testdata", ".whylogs_s3.yaml")
    return config_path


@pytest.fixture(scope="session")
def s3_all_config_path():
    config_path = os.path.join(_MY_DIR, os.pardir, "testdata", ".whylogs_s3_all.yaml")
    return config_path


@pytest.fixture(scope="session")
def mlflow_config_path():
    config_path = os.path.join(_MY_DIR, os.pardir, "testdata", ".whylogs_mlflow.yaml")
    return config_path


@pytest.fixture(scope="session")
def s3_transport_config_path():
    config_path = os.path.join(_MY_DIR, os.pardir, "testdata", ".whylogs_s3_transport_param.yaml")
    return config_path


@pytest.fixture(scope="session")
def s3_all_config_metadata_path():
    config_path = os.path.join(_MY_DIR, os.pardir, "testdata", ".whylogs_s3_metadata.yaml")
    return config_path


@pytest.fixture(scope="session")
def local_config_path():
    config_path = os.path.join(_MY_DIR, os.pardir, "testdata", ".whylogs_local.yaml")
    return config_path


@pytest.fixture(scope="session")
def df_lending_club():
    # just the top 50
    df = pd.read_csv(os.path.join(_MY_DIR, os.pardir, "testdata", "lending_club_1000.csv"))
    return df.head(50)


@pytest.fixture(scope="session")
def test_data_path():

    imag_path = os.path.join(_MY_DIR, os.pardir, "testdata")
    return imag_path


@pytest.fixture(scope="session")
def image_files():
    from os import listdir
    from os.path import isfile, join

    image_dir = os.path.join(_MY_DIR, os.pardir, "testdata", "images")
    image_files = [os.path.join(image_dir, f) for f in listdir(image_dir) if isfile(join(image_dir, f))]
    return sorted(image_files)


@pytest.fixture(scope="session")
def file_list():
    from os import listdir
    from os.path import isfile, join

    file_dir = os.path.join(_MY_DIR, os.pardir, "testdata", "files")
    file_list = [os.path.join(file_dir, f) for f in listdir(file_dir) if isfile(join(file_dir, f))]
    return sorted(file_list)
