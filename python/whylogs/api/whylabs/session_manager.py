import logging
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Dict, List, Optional, cast

import requests as r
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
from whylogs.api.whylabs.config import SessionConfig
from whylogs.api.whylabs.notebook_check import is_notebook
from whylogs.core.view.dataset_profile_view import DatasetProfileView

logger = logging.getLogger(__name__)


@dataclass
class UploadReferenceResult:
    viewing_url: str
    whylabs_response: BatchLogSessionReferenceResponse


class Session(ABC):
    @abstractmethod
    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> UploadReferenceResult:
        pass


class GuestSession(Session):
    def __init__(self, config: SessionConfig, whylabs_client: ApiClient) -> None:
        """
        Get a guest session that uses the id that is in the env, or the config if there is no env.
        If neither exist then this will attempt to create a new session and store the id in the config,
        which does require a successful service call to whylabs.
        """
        self._config = config
        self._whylabs_session_api = SessionsApi(whylabs_client)
        self._user_guid = self._get_or_create_user_guid()
        self._session_id = self._get_or_create_session_id()

        if is_notebook():
            print(f"Initialized anonymous session with id {self._session_id}")

    def _get_or_create_session_id(self) -> str:
        session_id = self._config.get_session_id()
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

    def _create_session_id(self) -> str:
        try:
            user_guid = self._user_guid
            response: CreateSessionResponse = self._whylabs_session_api.create_session(CreateSessionRequest(user_guid))
            logger.debug(f"Created session {response.id}")
            return response.id
        except ApiException as e:
            logger.error(e)
            raise e

    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> UploadReferenceResult:
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
                    raise ValueError(f"WhyLabs returned extra alias {ref.alias}. This is a WhyLabs bug.")

                result_set = profile_aliases[ref.alias]
                writables = result_set.get_writables()

                if writables is None:
                    logger.warning(f"ResultSet for alias {ref.alias} has no profiles. Skipping upload.")
                    continue

                if len(writables) == 1:
                    view = cast(DatasetProfileView, writables[0])  # TODO Jamie, is this right?
                    r.put(ref.upload_url, data=view.serialize())
                else:
                    logger.warning(f"Segments aren't supported in the log_reference api yet, skipping {ref.alias}")
                    continue

            return UploadReferenceResult(viewing_url=viewing_url, whylabs_response=response)
        except ApiException as e:
            logger.error(e)
            raise e


class ApiKeySession(Session):
    def __init__(self, config: SessionConfig) -> None:
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

        if is_notebook():
            print(f"Initialized whylabs session with for org {self.org_id}")

    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> UploadReferenceResult:
        # TODO support later
        raise NotImplementedError()


class SessionManager:
    __instance: Optional["SessionManager"] = None

    def __init__(self, anonymous: bool = False):
        self._config = SessionConfig()
        client_config = Configuration()
        client_config.host = self._config.get_whylabs_endpoint()
        self._whylabs_client = ApiClient(client_config)

        self.session: Session = (
            GuestSession(self._config, self._whylabs_client) if anonymous else ApiKeySession(self._config)
        )

    @staticmethod
    def init(anonymous: bool = False) -> None:
        if SessionManager.__instance is None:
            SessionManager.__instance = SessionManager(anonymous=anonymous)
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


def init(anonymous: bool = False, reinit: bool = False) -> None:
    if reinit:
        SessionManager.reset()

    try:
        SessionManager.init(anonymous=anonymous)
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
