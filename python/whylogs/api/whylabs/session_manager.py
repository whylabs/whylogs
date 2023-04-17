import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from whylabs_client.api.sessions_api import (
    CreateSessionRequest,
    CreateSessionResponse,
    SessionsApi,
)
from whylabs_client.api_client import ApiClient, Configuration

from .auth_file import get_auth_file_path
from .notebook_check import is_git_actions, is_notebook
from .variables import Variables

DEFAULT_PATH = os.getenv("WHYLOGS_CONFIG_PATH") or f"{Path.home()}/.whylabs/auth.ini"
_auth_path = get_auth_file_path(auth_path=Path(DEFAULT_PATH))

DEFAULT_WHYLABS_HOST = "https://api.whylabsapp.com"
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GuestSession:
    def __init__(self, session_id: str):
        self.session_id = session_id
        Variables.set_variable_to_config_file(key="session_id", value=self.session_id, auth_path=_auth_path)


@dataclass
class UserSession:
    org_id: str
    api_key: str

    def __post_init__(self):
        Variables.set_variable_to_config_file(key="org_id", value=self.org_id, auth_path=_auth_path)
        Variables.set_variable_to_config_file(key="api_key", value=self.api_key, auth_path=_auth_path)


def _create_session_id(user_id: str) -> str:
    config = Configuration()
    config.host = os.getenv("WHYLABS_HOST") or DEFAULT_WHYLABS_HOST

    client = ApiClient(config)
    api = SessionsApi(client)
    response: CreateSessionResponse = api.create_session(CreateSessionRequest(user_id))

    logger.debug(f"Created session {response.id}")
    return response.id


def _get_logged_session(auth_path: Path = _auth_path) -> UserSession:
    api_key = os.getenv("WHYLABS_API_KEY") or Variables.get_variable_from_config_file(
        auth_path=auth_path, key="api_key"
    )
    org_id = os.getenv("ORG_ID") or Variables.get_variable_from_config_file(auth_path=auth_path, key="org_id")

    if is_notebook() and not is_git_actions():
        api_key = api_key or Variables.get_variable_from_input(variable_name="api_key")
        org_id = org_id or Variables.get_variable_from_input(variable_name="org_id")

    if api_key is None or org_id is None:
        raise ValueError(
            f"You must define your WHYLABS_API_KEY and ORG_ID environment variables,"
            f" or set them on an ini file on {auth_path}"
        )

    return UserSession(org_id=org_id, api_key=api_key)


def _get_guest_session() -> GuestSession:
    session_id = Variables.get_variable_from_config_file(auth_path=_auth_path, key="session_id")
    if session_id is None:
        user_id = str(uuid.uuid4())
        session_id = _create_session_id(user_id=user_id)
    return GuestSession(session_id=session_id)


def create_session(anonymous: Optional[bool] = None) -> Union[GuestSession, UserSession]:
    if not anonymous:
        return _get_logged_session()
    else:
        return _get_guest_session()


class SessionManager:
    __instance = None

    def __init__(self, anonymous: Optional[bool] = None):
        self._anonymous = anonymous
        if SessionManager.__instance is not None:
            raise Exception("There is an active Session, use Session.get_instance() instead")
        else:
            SessionManager.__instance = self
            self.session = create_session(anonymous=self._anonymous)

    @staticmethod
    def get_instance(anonymous: Optional[bool] = None):
        if SessionManager.__instance is None:
            SessionManager(anonymous=anonymous)
        return SessionManager.__instance

    @staticmethod
    def is_active() -> bool:
        if SessionManager.__instance is None:
            return False
        return True


def init(anonymous: Optional[bool] = None) -> None:
    SessionManager.get_instance(anonymous=anonymous)
