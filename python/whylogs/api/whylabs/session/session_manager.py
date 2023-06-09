import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from enum import Enum
from typing import Dict, List, Optional, Union

import requests as web_requests
from whylabs_client import ApiException  # type: ignore
from whylabs_client.api.sessions_api import (  # type: ignore
    BatchLogReferenceRequest,
    BatchLogSessionReferenceResponse,
    CreateSessionRequest,
    CreateSessionResponse,
    LogReferenceRequest,
    LogSessionReferenceResponse,
    SessionsApi,
)
from whylabs_client.api_client import ApiClient, Configuration  # type: ignore

from whylogs.api.logger.result_set import ResultSet
from whylogs.api.whylabs.session.config import SessionConfig
from whylogs.api.whylabs.session.notebook_check import is_notebook
from whylogs.api.whylabs.session.session_types import (
    NotSupported,
    init_notebook_logging,
    log_if_notebook,
)
from whylogs.migration.uncompound import _uncompound_dataset_profile

logger = logging.getLogger(__name__)


class SessionType(Enum):
    WHYLABS_ANONYMOUS = "whylabs_anonymous"
    WHYLABS = "whylabs"
    LOCAL = "local"


@dataclass
class UploadReferenceResult:
    viewing_url: str
    whylabs_response: BatchLogSessionReferenceResponse


class Session(ABC):
    @abstractmethod
    def get_type(self) -> SessionType:
        raise NotImplementedError()

    @abstractmethod
    def upload_reference_profiles(
        self, profile_aliases: Dict[str, ResultSet]
    ) -> Union[UploadReferenceResult, NotSupported]:
        raise NotImplementedError()


class GuestSession(Session):
    def __init__(self, config: SessionConfig, whylabs_client: ApiClient) -> None:
        """
        Get a guest session that uses the id that is in the env, or the config if there is no env.
        If neither exist then this will attempt to create a new session and store the id in the config,
        which does require a successful service call to whylabs.
        """
        from whylogs.api.usage_stats import emit_usage

        self._config = config
        self._whylabs_session_api = SessionsApi(whylabs_client)
        self._user_guid = self._get_or_create_user_guid()
        self._session_id = self._get_or_create_session_id()
        config_path = config.get_config_file_path()
        emit_usage("guest_session")
        log_if_notebook(f"Initialized anonymous session with id {self._session_id} in config {config_path}")

    def get_type(self) -> SessionType:
        return SessionType.WHYLABS_ANONYMOUS

    def _validate_config_session(self) -> Optional[str]:
        """
        Look up the current config value for the session and make sure it's still valid by
        calling WhyLabs. If it's not valid, remove it from the config and return None.
        """
        session_id = self._config.get_session_id()

        if session_id is None:
            return None

        if not self._validate_session_id(session_id):
            self._config.remove_session_id()
            log_if_notebook(
                f"⚠️ Session {session_id} is no longer valid, generating a new one. If you want to upload to your WhyLabs "
                "account then remove why.init() and use https://docs.whylabs.ai/docs/whylabs-onboarding/#getting-started-in-whylabs."
            )
            return None
        else:
            return session_id

    def _get_or_create_session_id(self) -> str:
        session_id = self._validate_config_session()

        if session_id is None:
            session_id = self._create_session_id()
            self._config.set_session_id(session_id)

        return session_id

    def _get_or_create_user_guid(self) -> str:
        user_guid = self._config.get_user_guid()

        if user_guid is None:
            user_guid = str(uuid.uuid4())
            self._config.set_user_guid(user_guid)

        return user_guid

    def _validate_session_id(self, session_id: str) -> bool:
        """
        Check to see if the session id is valid by calling WhyLabs.
        """
        try:
            request = BatchLogReferenceRequest(
                session_id=session_id, references=[LogReferenceRequest(alias="test", datasetTimestamp=0)]
            )
            self._whylabs_session_api.batch_create_reference_profile_upload(
                batch_log_reference_request=request, session_id=session_id
            )
            return True
        except ApiException:
            return False

    def _create_session_id(self) -> str:
        try:
            user_guid = self._user_guid
            response: CreateSessionResponse = self._whylabs_session_api.create_session(CreateSessionRequest(user_guid))
            logger.debug(f"Created session {response.id}")
            return response.id
        except ApiException as e:
            logger.error(e)
            raise e

    def upload_reference_profiles(
        self, profile_aliases: Dict[str, ResultSet]
    ) -> Union[UploadReferenceResult, NotSupported]:
        requests: List[LogReferenceRequest] = []
        for alias, _profile in profile_aliases.items():
            requests.append(LogReferenceRequest(alias=alias, datasetTimestamp=0))

        try:
            request = BatchLogReferenceRequest(session_id=self._session_id, references=requests)
            response: BatchLogSessionReferenceResponse = (
                self._whylabs_session_api.batch_create_reference_profile_upload(
                    batch_log_reference_request=request, session_id=self._session_id
                )
            )

            viewing_url: str = response.observatory_url  # url where the profiles can be viewed
            references: List[LogSessionReferenceResponse] = response.references

            for ref in references:
                if ref.alias not in profile_aliases:
                    # Should not be possible. WhyLabs api should echo each of the supplied aliases back
                    logger.warning(f"WhyLabs returned extra alias {ref.alias}. This is a WhyLabs bug.")
                    continue

                result_set = profile_aliases[ref.alias]
                if hasattr(result_set, "segments"):
                    logger.warning(f"Segments aren't supported in the log_reference api yet, skipping {ref.alias}")
                    continue
                view_v1 = result_set.view()
                if view_v1 is None:
                    logger.warning(f"skipping {ref.alias} because it didn't contain a profile view")
                    continue
                whylabs_compatible_view = _uncompound_dataset_profile(view_v1)
                web_requests.put(ref.upload_url, data=whylabs_compatible_view.serialize())

            time.sleep(2)
            return UploadReferenceResult(viewing_url=viewing_url, whylabs_response=response)
        except ApiException as e:
            logger.error(e)
            raise e


class LocalSession(Session):
    def get_type(self) -> SessionType:
        return SessionType.LOCAL

    def upload_reference_profiles(
        self, profile_aliases: Dict[str, ResultSet]
    ) -> Union[UploadReferenceResult, NotSupported]:
        return NotSupported()


class ApiKeySession(Session):
    def __init__(self, config: SessionConfig) -> None:
        from whylogs.api.usage_stats import emit_usage

        self.api_key = config.get_api_key()
        self.org_id = config.get_org_id()

        if self.api_key is None and is_notebook():
            self.api_key = config.get_or_prompt_api_key(persist=True)
        else:
            logger.warning("No api key found in session or configuration, will not be able to send data to whylabs.")

        if self.org_id is None and is_notebook():
            self.org_id = config.get_or_prompt_org_id(persist=True)
        else:
            logger.warning("No org id found in session or configuration, will not be able to send data to whylabs.")
        emit_usage("api_key_session")
        if is_notebook():
            log_if_notebook(f"Initialized whylabs session with for org {self.org_id}")

    def get_type(self) -> SessionType:
        return SessionType.WHYLABS

    def upload_reference_profiles(
        self, profile_aliases: Dict[str, ResultSet]
    ) -> Union[UploadReferenceResult, NotSupported]:
        # TODO support soon
        return NotSupported()


class SessionManager:
    __instance: Optional["SessionManager"] = None

    def __init__(self, type: SessionType = SessionType.LOCAL):
        self._config = SessionConfig()
        client_config = Configuration()
        client_config.host = self._config.get_whylabs_endpoint()
        self._whylabs_client = ApiClient(client_config)

        self.session: Optional[Session] = None
        if type == SessionType.LOCAL:
            pass
        elif type == SessionType.WHYLABS_ANONYMOUS:
            self.session = GuestSession(self._config, self._whylabs_client)
        elif type == SessionType.WHYLABS:
            self.session = ApiKeySession(self._config)

    @staticmethod
    def init(type: SessionType = SessionType.LOCAL) -> None:
        if SessionManager.__instance is None:
            init_notebook_logging()
            SessionManager.__instance = SessionManager(type=type)
        else:
            logger.warning("SessionManager is already initialized. Ignoring call to init()")

    @staticmethod
    def reset() -> None:
        SessionManager.__instance = None

    @staticmethod
    def get_instance() -> Optional["SessionManager"]:
        return SessionManager.__instance

    @staticmethod
    def is_active() -> bool:
        return SessionManager.get_instance() is not None


def init(session_type: Union[SessionType, str] = SessionType.LOCAL, reinit: bool = False) -> None:
    if reinit:
        SessionManager.reset()

    if isinstance(session_type, str):
        try:
            _session_type = SessionType[session_type.upper()]
        except KeyError:
            logger.error(f"Invalid session type {session_type}. Valid values are {SessionType.__members__.keys()}")
            return
    else:
        _session_type = session_type

    try:
        SessionManager.init(type=_session_type)
    except PermissionError as e:
        logger.warning("Could not create or read configuration file for session. Profiles won't be uploaded.", e)
    except Exception as e:
        logger.warning("Could not initialize session", e)


def get_current_session() -> Optional[Session]:
    manager = SessionManager.get_instance()
    if manager is not None:
        return manager.session

    logger.warning("No session is initialized. Call whylogs.init(anonymous=True) to create a guest session.")
    return None
