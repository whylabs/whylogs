import logging
import uuid
from typing import List, Optional, Dict

from whylabs_client import ApiException  # type: ignore
from whylabs_client.api.sessions_api import (  # type: ignore
    CreateSessionRequest,
    LogReferenceRequest,
    BatchLogReferenceRequest,
    BatchLogSessionReferenceResponse,
    CreateSessionResponse,
    SessionsApi,
)
from whylabs_client.api_client import ApiClient, Configuration
from whylogs.api.logger.result_set import ResultSet  # type: ignore

from whylogs.api.whylabs.config import SessionConfig
from whylogs.core.dataset_profile import DatasetProfile
from abc import ABC, abstractmethod


logger = logging.getLogger(__name__)


class SessionAPI(ABC):
    @abstractmethod
    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> None:
        pass


class GuestSession(SessionAPI):
    def __init__(self, config: SessionConfig, whylabs_client: ApiClient) -> None:
        """
        Get a guest session that uses the id that is in the env, or the config if there is no env.
        If neither exist then this will attempt to create a new session and store the id in the config,
        which does require a successful service call to whylabs.
        """
        session_id = config.get_session_id()
        self._config = config
        self._whylabs_session_api = SessionsApi(whylabs_client)
        if session_id is None:
            # TODO throws, need to determine what to do here without interrupting user session
            session_id = self._create_session_id()
            config.set_session_id(session_id)

        self.session_id = session_id

    def _create_session_id(self) -> str:
        try:
            response: CreateSessionResponse = self._whylabs_session_api.create_session(
                CreateSessionRequest(str(uuid.uuid4()))
            )
            logger.debug(f"Created session {response.id}")
            return response.id
        except ApiException as e:
            logger.error(e)
            raise e

    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> None:
        requests: List[LogReferenceRequest] = []
        for alias, _profile in profile_aliases.items():
            requests.append(LogReferenceRequest(alias=alias, datasetTimestamp=0))

        try:
            request = BatchLogReferenceRequest(session_id=self.session_id, references=requests)
            response: BatchLogSessionReferenceResponse = (
                self._whylabs_session_api.batch_create_reference_profile_upload(
                    batch_log_reference_request=request, session_id=self.session_id
                )
            )

            url: str = response.observatory_url  # url where the profiles can be viewed
            print(response)
        except ApiException as e:
            logger.error(e)
            raise e


class ApiKeySession:
    def __init__(self, config: SessionConfig) -> None:
        self.api_key = config.get_or_prompt_api_key()
        self.org_id = config.get_or_prompt_org_id()

        # TODO if these are none do we ever want to persist them?

        if self.api_key is None:
            logger.warning("No api key found, will not be able to send data to whylabs")

        if self.org_id is None:
            logger.warning("No org id found, will not be able to send data to whylabs")

    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> None:
        # TODO support later
        raise NotImplementedError()


class Session(SessionAPI):
    guest_session: Optional[GuestSession]
    api_key_session: Optional[ApiKeySession]

    def __init__(self, guest_session: Optional[GuestSession], api_key_session: Optional[ApiKeySession]):
        self.guest_session = guest_session
        self.api_key_session = api_key_session

    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> None:
        if self.guest_session:
            self.guest_session.upload_reference_profiles(profile_aliases)
        elif self.api_key_session:
            self.api_key_session.upload_reference_profiles(profile_aliases)
        else:
            raise Exception("No session found, cannot upload reference profiles")


class SessionManager:
    __instance: Optional["SessionManager"] = None

    def __init__(self, anonymous: Optional[bool] = None):
        self._config = SessionConfig()

        client_config = Configuration()
        client_config.host = self._config.get_whylabs_endpoint()
        self._whylabs_client = ApiClient(client_config)

        if not anonymous:
            self.session = Session(api_key_session=ApiKeySession(self._config), guest_session=None)
        else:
            self.session = Session(api_key_session=None, guest_session=GuestSession(self._config, self._whylabs_client))

    @staticmethod
    def init(anonymous: Optional[bool] = None) -> None:
        if SessionManager.__instance is None:
            SessionManager.__instance = SessionManager(anonymous=anonymous)

        logger.warning("SessionManager is already initialized. Ignoring call to init()")

    @staticmethod
    def get_instance() -> Optional["SessionManager"]:
        return SessionManager.__instance

    @staticmethod
    def is_active() -> bool:
        return SessionManager.get_instance() is not None


#  TODO consider allowing reinitialization, it's annoying to reload the entire notebook because you mess this up
def init(anonymous: Optional[bool] = None) -> None:
    try:
        SessionManager.init(anonymous=anonymous)
    except Exception as e:
        logger.warning(f"Could not initialize session: {e}")


def get_current_session() -> Optional[Session]:
    manager = SessionManager.get_instance()
    if manager is not None:
        return manager.session

    logger.warning("No session is initialized. Call whylogs.init(anonymous=True) to create a guest session.")
    return None
