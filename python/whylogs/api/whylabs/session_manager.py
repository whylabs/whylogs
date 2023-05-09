import logging
import os
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union

from whylabs_client import ApiException  # type: ignore
from whylabs_client.api.sessions_api import (  # type: ignore
    CreateSessionRequest,
    CreateSessionResponse,
    SessionsApi,
)
from whylabs_client.api_client import ApiClient, Configuration  # type: ignore

from whylogs.api.whylabs.auth_file import get_auth_file_path
from whylogs.api.whylabs.notebook_check import is_notebook
from whylogs.api.whylabs.variables import Variables

DEFAULT_WHYLABS_HOST = "https://api.whylabsapp.com"
logger = logging.getLogger(__name__)


@dataclass
class GuestSession:
    session_id: str


@dataclass
class ApiKeySession:
    org_id: str
    api_key: str


def _get_default_authentication_path() -> Path:
    default_path = os.getenv("WHYLOGS_CONFIG_PATH") or f"{Path.home()}/.whylabs/auth.ini"
    auth_path = get_auth_file_path(auth_path=Path(default_path))
    return auth_path


def _create_session_id(user_guid: str) -> str:
    config = Configuration()
    config.host = os.getenv("WHYLABS_API_ENDPOINT") or DEFAULT_WHYLABS_HOST

    try:
        client = ApiClient(config)
        api = SessionsApi(client)
        response: CreateSessionResponse = api.create_session(CreateSessionRequest(user_guid))

        logger.debug(f"Created session {response.id}")
        return response.id
    except ApiException as e:
        logger.error(e)
        raise e


def _get_logged_session(auth_path: Optional[Path] = None) -> ApiKeySession:
    api_key = os.getenv("WHYLABS_API_KEY")
    org_id = os.getenv("ORG_ID")

    if api_key is None or org_id is None:
        _auth_path = _get_default_authentication_path()
        api_key = api_key or Variables.get_variable_from_config_file(auth_path=auth_path, key="api_key")
        org_id = org_id or Variables.get_variable_from_config_file(auth_path=auth_path, key="org_id")

        if is_notebook():
            if api_key is None:
                api_key = Variables.get_variable_from_getpass(variable_name="api_key")
                Variables.set_variable_to_config_file(key="api_key", value=api_key, auth_path=_auth_path)
            if org_id is None:
                org_id = Variables.get_variable_from_input(variable_name="org_id")
                Variables.set_variable_to_config_file(key="org_id", value=org_id, auth_path=_auth_path)

    if api_key is None or org_id is None:
        raise ValueError(
            f"You must define your WHYLABS_API_KEY and ORG_ID environment variables,"
            f" or set them on a config file on {auth_path}"
        )

    return ApiKeySession(org_id=org_id, api_key=api_key)


def _get_or_create_guest_session() -> GuestSession:
    auth_path = _get_default_authentication_path()
    session_id = Variables.get_variable_from_config_file(auth_path=auth_path, key="session_id")
    if session_id is None:
        user_guid = str(uuid.uuid4())
        session_id = _create_session_id(user_guid=user_guid)
        Variables.set_variable_to_config_file(key="session_id", value=session_id, auth_path=auth_path)
    return GuestSession(session_id=session_id)


def _get_or_create_session(anonymous: Optional[bool] = None) -> Union[GuestSession, ApiKeySession]:
    if not anonymous:
        return _get_logged_session()
    else:
        return _get_or_create_guest_session()


class SessionManager:
    __instance: Optional['SessionManager'] = None

    def __init__(self, anonymous: Optional[bool] = None):
        if SessionManager.__instance is not None:
            raise Exception("There is an active Session, use get_instance() instead")

        self.session = _get_or_create_session(anonymous=anonymous)

    @staticmethod
    def get_instance(anonymous: Optional[bool] = None) -> 'SessionManager':
        if SessionManager.__instance is not None:
            return SessionManager.__instance

        manager = SessionManager(anonymous=anonymous)
        SessionManager.__instance = manager
        return manager

    @staticmethod
    def is_active() -> bool:
        if SessionManager.__instance is None:
            return False
        return True


def init(anonymous: Optional[bool] = None) -> None:
    SessionManager.get_instance(anonymous=anonymous)
