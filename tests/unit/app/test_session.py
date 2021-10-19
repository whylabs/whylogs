import pytest

from whylogs.app.config import SessionConfig
from whylogs.app.session import (
    Session,
    get_or_create_session,
    get_session,
    reset_default_session,
    session_from_config,
)


def test_get_global_session():
    session = get_or_create_session()

    global_session = get_session()

    assert session == global_session


def test_reset():

    get_or_create_session()
    reset_default_session()
    global_session = get_session()
    assert global_session.project is not None


def test_session_log_dataframe(df):
    pass

    session = session_from_config(SessionConfig("default-project", "default-pipeline", [], False))
    session.log_dataframe(df)

    assert session.logger() is not None

    assert session.logger("default-project").dataset_name == "default-project"


def test_session_profile(df):

    session = session_from_config(SessionConfig("default-project", "default-pipeline", [], False))
    profile = session.log_dataframe(df)
    assert profile is not None

    summary = profile.flat_summary()

    flat_summary = summary["summary"]
    assert len(flat_summary) == 4


def test_profile_df(df):
    session = get_or_create_session()
    log_profile = session.log_dataframe(df)
    profile = session.profile_dataframe(df)

    assert log_profile.name == profile.name
    assert log_profile.dataset_timestamp == profile.dataset_timestamp
    assert log_profile.session_timestamp == profile.session_timestamp

    assert len(profile.columns) == 4
    assert len(log_profile.tags) == 1
    assert len(profile.tags) == 2


def test_close_session(df):
    session = get_or_create_session()
    session.close()
    assert session.is_active() == False
    log_profile = session.log_dataframe(df)
    assert log_profile == None
    profile = session.profile_dataframe(df)
    assert profile == None
    profile = session.new_profile(df)
    assert profile == None

    with pytest.raises(RuntimeError):
        session.logger()


def test_session_default():
    session = Session()
    assert session.is_active() == True, "Newly created default session is expected to be active."
    assert session.project == "", "project should be optional and default to empty string."
    assert session.pipeline == "", "pipeline should be optional and default to empty string."


def test_logger_cache():
    session = get_or_create_session()
    with session.logger("cache-test", with_rotation_time="s") as logger:
        logger.log({"name": 1})
    session.close()


def test_remove_logger():
    session = get_or_create_session()
    session.logger("default-project")
    with pytest.raises(KeyError):
        session.remove_logger("test")
