from typing import cast

import pytest

from whylogs.api.whylabs.session.config import InitException
from whylogs.api.whylabs.session.session import Session
from whylogs.api.whylabs.session.session_manager import get_current_session, init
from whylogs.api.whylabs.session.session_types import SessionType

_config_path = "/tmp/whylogs.ini"

# FYI, these tests do require network to work. They call whylabs to generate session ids.


def test_whylabs_works() -> None:
    init(config_path=_config_path, allow_anonymous=True, allow_local=True, whylabs_api_key="key", reinit=True)
    session = cast(Session, get_current_session())
    assert session.get_type() == SessionType.WHYLABS


def test_allow_true_anonymous_works() -> None:
    init(config_path=_config_path, allow_anonymous=True, allow_local=False, reinit=True)
    session = cast(Session, get_current_session())
    assert session.get_type() == SessionType.WHYLABS_ANONYMOUS


def test_allow_false_anonymous_works() -> None:
    # Will throw because it has no info to infer a session type with, and everything is not allowed
    with pytest.raises(InitException):
        init(config_path=_config_path, allow_anonymous=False, allow_local=False, reinit=True)


def test_allow_true_local_works() -> None:
    init(config_path=_config_path, allow_anonymous=False, allow_local=True, reinit=True)

    session = cast(Session, get_current_session())
    assert session.get_type() == SessionType.LOCAL


def test_allow_false_anonymous_keeps_working() -> None:
    # First init as anonymous
    init(config_path=_config_path, allow_anonymous=True, allow_local=False, reinit=True)
    # Then disable init and expect it to work
    init(config_path=_config_path, allow_anonymous=False, allow_local=True, reinit=True)

    session = cast(Session, get_current_session())
    assert session.get_type() == SessionType.LOCAL
