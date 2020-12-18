

import pytest
from whylogs.app.session import  get_or_create_session,get_session,get_logger,reset_default_session,session_from_config
from whylogs.app.config import SessionConfig
from whylogs.app.session import Session
from pandas import util


def test_get_global_session():
    _session=None
    session = get_or_create_session()

    global_session= get_session()

    assert session== global_session


def test_reset():
    
    session = get_or_create_session()
    reset_default_session()
    global_session= get_session()
    assert  global_session.project is not None


def test_session_log_dataframe():
    _session=None
    
    session = session_from_config(SessionConfig(
                "default-project", "default-pipeline", [], False
            ))
    df= util.testing.makeDataFrame()
    profile=session.log_dataframe(df)

    
    assert session.logger() is not None

    assert session.logger("default-project").dataset_name == "default-project"

def test_session_profile():
    

    session = session_from_config(SessionConfig(
                "default-project", "default-pipeline", [], False
            ))
    df= util.testing.makeDataFrame()
    profile=session.log_dataframe(df)
    assert profile is not None

    summary = profile.flat_summary()
    

    flat_summary = summary['summary']
    assert len(flat_summary) == 4

def test_profile_df():
    session = get_or_create_session()
    df= util.testing.makeDataFrame()
    log_profile=session.log_dataframe(df)
    profile=session.profile_dataframe(df)

    assert log_profile.name == profile.name
    assert log_profile.dataset_timestamp == profile.dataset_timestamp
    assert log_profile.session_timestamp == profile.session_timestamp
    
    
    assert len(profile.columns) ==4
    assert len(log_profile.tags) == 1
    assert len(profile.tags) == 2
 

def test_close_session():
    session = get_or_create_session()
    session.close()
    assert session.is_active() == False
    df= util.testing.makeDataFrame()
    log_profile=session.log_dataframe(df)
    assert log_profile == None
    profile=session.profile_dataframe(df)
    assert profile == None
    profile=session.new_profile(df)
    assert profile == None

    with pytest.raises(RuntimeError):
        session.logger()


def test_remove_logger():
    session = get_or_create_session()
    session.logger("default-project")
    with pytest.raises(KeyError):
        session.remove_logger("test")
