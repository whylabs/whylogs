import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

import keyring
from whylabs_client.api.sessions_api import (
    CreateSessionRequest,
    CreateSessionResponse,
    SessionsApi,
)
from whylabs_client.api_client import ApiClient, Configuration

from .auth_file import get_auth_file_path, get_configparser_object
from .notebook_check import is_notebook
from .variables import Variables

_auth_path = get_auth_file_path(auth_path=Path(f"{Path.home()}/.whylabs/auth.ini"))
DEFAULT_WHYLABS_HOST = "https://api.whylabsapp.com/"
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
        if not is_notebook():
            Variables.set_variable_to_config_file(key="org_id", value=self.org_id, auth_path=_auth_path)
            keyring.set_password("whylabs", "api_key", self.api_key)


def _create_session_id(user_id: str) -> str:
    config = Configuration()
    config.host = os.getenv("WHYLABS_HOST") or DEFAULT_WHYLABS_HOST

    client = ApiClient(config)
    api = SessionsApi(client)
    response: CreateSessionResponse = api.create_session(CreateSessionRequest(user_id))

    logger.info(f"Created session {response.id}")
    return response.id


def _get_logged_session() -> UserSession:
    api_key = os.getenv("WHYLABS_API_KEY")
    org_id = os.getenv("ORG_ID")

    if is_notebook():
        api_key = api_key or Variables.get_variable_from_input(variable_name="api_key")
        org_id = org_id or Variables.get_variable_from_input(variable_name="org_id")

    if not is_notebook():
        config_object = get_configparser_object(auth_path=_auth_path)
        api_key = api_key or Variables.get_password_from_keyring()
        org_id = org_id or Variables.get_variable_from_config_file(config=config_object, key="org_id")

    if api_key is None or org_id is None:
        raise ValueError("You must define your WHYLABS_API_KEY and ORG_ID environment variables")

    return UserSession(org_id=org_id, api_key=api_key)


def _get_guest_session() -> GuestSession:
    config = get_configparser_object(auth_path=_auth_path)
    session_id = Variables.get_variable_from_config_file(config=config, key="session_id")
    if session_id is None:
        user_id = str(uuid.uuid4())
        session_id = _create_session_id(user_id=user_id)
    return GuestSession(session_id=session_id)


def create_session(anonymous: Optional[bool] = None) -> Union[GuestSession, UserSession]:
    if is_notebook() is True:
        while True:
            anonymous_input = input("Do you want to create an anonymous session? [True/False] (default: False) ")
            if anonymous_input.lower() == "true":
                return _get_guest_session()
            elif anonymous_input.lower() in ["false", ""]:
                return _get_logged_session()
            else:
                logger.error("To login, let us know if you want to create an anonymous session!")

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


def init(anonymous: bool = False) -> None:
    SessionManager.get_instance(anonymous=anonymous)
