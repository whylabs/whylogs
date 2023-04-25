import configparser
import os
import uuid

import pytest

from whylogs.api.whylabs.auth_file import _create_blank_auth_file
from whylogs.api.whylabs.session_manager import (
    GuestSession,
    SessionManager,
    UserSession,
    _auth_path,
    _create_session_id,
    _get_guest_session,
    _get_logged_session,
    create_session,
)


class TestSessionManager:
    @classmethod
    def setup_class(cls):
        os.environ["WHYLABS_API_KEY"] = "api_key"
        os.environ["ORG_ID"] = "org_id"

    @classmethod
    def teardown_class(cls):
        del os.environ["WHYLABS_API_KEY"]
        del os.environ["ORG_ID"]
        os.remove(_auth_path)
        os.rmdir(_auth_path.parent)

    def test_create_session_id(self):
        user_id = str(uuid.uuid4())
        session_id = _create_session_id(user_id=user_id)
        assert "session-" in session_id

    def test_get_guest_session(self):
        session = _get_guest_session()
        assert isinstance(session, GuestSession)
        assert "session-" in session.session_id

        config = configparser.ConfigParser()
        config.read(_auth_path)

        assert config.get("whylabs", "session_id") == session.session_id

        new_session = _get_guest_session()
        assert new_session.session_id == session.session_id

    def test_logged_session_with_env_var(self):
        session = _get_logged_session()

        assert isinstance(session, UserSession)
        assert session.org_id == "org_id"
        assert session.api_key == "api_key"

    def test_create_session(self):
        logged_session = create_session(anonymous=False)
        assert isinstance(logged_session, UserSession)

        anon_session = create_session(anonymous=True)
        assert isinstance(anon_session, GuestSession)

    def test_session_manager_singleton(self):
        sm = SessionManager.get_instance(anonymous=False)
        assert isinstance(sm.session, UserSession)

        new_sm = SessionManager.get_instance(anonymous=True)
        assert new_sm == sm
        other_sm = SessionManager.get_instance(anonymous=False)
        assert other_sm == sm

        with pytest.raises(Exception):
            SessionManager()

    def test_why_init(self):
        import whylogs as why

        why.init()

        with pytest.raises(Exception):
            SessionManager(anonymous=False)

    def test_why_init_anonymous(self):
        import whylogs as why

        why.init(anonymous=True)

        config = configparser.ConfigParser()
        config.read(_auth_path)
        assert config.get("whylabs", "session_id")


def test_logged_session_with_config_file():
    _create_blank_auth_file(auth_path=_auth_path)
    config = configparser.ConfigParser()
    config.read(_auth_path)
    config.set("whylabs", "api_key", "my_api_key")
    config.set("whylabs", "org_id", "my_org_id")
    with open(_auth_path, "w") as configfile:
        config.write(configfile)

    session = _get_logged_session(auth_path=_auth_path)

    assert session.org_id == "my_org_id"
    assert session.api_key == "my_api_key"
