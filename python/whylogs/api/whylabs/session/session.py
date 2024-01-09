import logging
import time
import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from typing import Any, Dict, List, Optional, Set, Union, cast

import requests as web_requests
from whylabs_client import ApiException  # type: ignore
from whylabs_client.api.log_api import (  # type: ignore
    GetProfileObservatoryLinkRequest,
    GetProfileObservatoryLinkResponse,
    LogApi,
)
from whylabs_client.api.sessions_api import (  # type: ignore
    BatchLogReferenceRequest,
    BatchLogSessionReferenceResponse,
    CreateDatasetProfileUploadResponse,
    CreateSessionRequest,
    CreateSessionResponse,
    LogAsyncRequest,
    LogReferenceRequest,
    LogSessionReferenceResponse,
    SessionsApi,
)

from whylogs.api.logger.result_set import ResultSet
from whylogs.api.whylabs.session.config import INIT_DOCS, SessionConfig
from whylogs.api.whylabs.session.lazy import Lazy
from whylogs.api.whylabs.session.session_types import InteractiveLogger as il
from whylogs.api.whylabs.session.session_types import NotSupported, SessionType
from whylogs.api.whylabs.session.whylabs_client_cache import WhylabsClientCache
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.migration.uncompound import _uncompound_dataset_profile  # type: ignore

logger = logging.getLogger(__name__)


@dataclass
class UploadResult:
    result: Optional[Any]
    viewing_url: str

    """
    The url to view uploaded profiles individually if this was a batch upload
    """
    individual_viewing_urls: Optional[List[str]] = None


class Session(ABC):
    def __init__(self, session_config: SessionConfig) -> None:
        self.config = session_config
        WhylabsClientCache._WhylabsClientCache__init_instance()  # type: ignore
        super().__init__()

    def get_type(self) -> SessionType:
        return self.get_config().session_type

    def get_config(self) -> SessionConfig:
        return self.config

    @abstractmethod
    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> Union[UploadResult, NotSupported]:
        raise NotImplementedError()

    @abstractmethod
    def upload_batch_profile(self, result_set: ResultSet) -> Union[UploadResult, NotSupported]:
        raise NotImplementedError()


class GuestSession(Session):
    def __init__(self, config: SessionConfig) -> None:
        """
        Get a guest session that uses the id that is in the env, or the config if there is no env.
        If neither exist then this will attempt to create a new session and store the id in the config,
        which does require a successful service call to whylabs.
        """
        from whylogs.api.usage_stats import emit_usage

        super().__init__(config)

        # Using lazy initialization to work around circular dependency issues
        self._whylabs_session_api = Lazy(self.__create_session_api)
        self._user_guid = self._get_or_create_user_guid()
        emit_usage("guest_session")

    def __create_session_api(self) -> SessionsApi:
        from whylogs.api.whylabs.session.whylabs_client_cache import ClientCacheConfig

        fake_key = "xxxxxxxxxx.xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
        client, _ = WhylabsClientCache.instance().get_client(
            ClientCacheConfig(
                api_key=fake_key,  # Key doesn't matter for anonymous sessions
            )
        )

        return SessionsApi(client)

    def _validate_config_session(self) -> Optional[str]:
        """
        Look up the current config value for the session and make sure it's still valid by
        calling WhyLabs. If it's not valid, remove it from the config and return None.
        """
        session_id = self.config.get_session_id()

        if session_id is None:
            return None

        if not self._validate_session_id(session_id):
            self.config.remove_session_id()
            il.warning(
                f"Session {session_id} is no longer valid, generating a new one. If you want to upload to your WhyLabs "
                "account then authenticate with an api token using `python -m whylogs.api.whylabs.session.why_init`."
                f"See {INIT_DOCS}"
            )
            return None
        else:
            return session_id

    def _get_or_create_session_id(self) -> str:
        session_id = self._validate_config_session()

        if session_id is None:
            session_id = self._create_session_id()
            self.config.set_session_id(session_id)

        return session_id

    def _get_or_create_user_guid(self) -> str:
        user_guid = self.config.get_user_guid()

        if user_guid is None:
            user_guid = str(uuid.uuid4())
            self.config.set_user_guid(user_guid)

        return user_guid

    def _validate_session_id(self, session_id: str) -> bool:
        """
        Check to see if the session id is valid by calling WhyLabs.
        """
        try:
            request = BatchLogReferenceRequest(  # type: ignore
                session_id=session_id, references=[LogReferenceRequest(alias="test", datasetTimestamp=0)]
            )
            self._whylabs_session_api.value.batch_create_reference_profile_upload(  # type: ignore
                batch_log_reference_request=request, session_id=session_id
            )
            return True
        except ApiException:
            return False

    def _create_session_id(self) -> str:
        try:
            user_guid = self._user_guid
            response: CreateSessionResponse = self._whylabs_session_api.value.create_session(  # type: ignore
                CreateSessionRequest(user_guid)
            )
            logger.debug(f"Created session {response.id}")  # type: ignore
            return response.id  # type: ignore
        except ApiException as e:
            logger.error(e)
            raise e

    def upload_batch_profile(self, result_set: ResultSet) -> Union[UploadResult, NotSupported]:
        if len(result_set.get_writables() or []) > 1:
            il.warning(
                "Segmented profiles are not supported in anonymous sessions. Create a free WhyLabs account to upload segments."
            )
            return NotSupported()

        profile = result_set.view()
        if profile is None:
            raise Exception("Profile did not contain any data.")
        try:
            if profile._dataset_timestamp is not None:  # type: ignore
                timestamp = int(profile._dataset_timestamp.timestamp() * 1000)  # type: ignore
            else:
                timestamp = int(datetime.now(timezone.utc).timestamp() * 1000)

            request = LogAsyncRequest(datasetTimestamp=timestamp, dataset="model-1", segment_tags=[])
            session_id = self._get_or_create_session_id()
            response: CreateDatasetProfileUploadResponse = (
                self._whylabs_session_api.value.create_dataset_profile_upload(session_id, request)
            )
            viewing_url: str = response.observatory_url
            upload_url: str = response.upload_url

            whylabs_compatible_view = _uncompound_dataset_profile(profile)
            web_requests.put(upload_url, data=whylabs_compatible_view.serialize())
            time.sleep(2)
            return UploadResult(viewing_url=viewing_url, result=response)
        except ApiException as e:
            logger.error(e)
            raise e

    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> Union[UploadResult, NotSupported]:
        requests: List[LogReferenceRequest] = []
        for alias, _profile in profile_aliases.items():
            requests.append(LogReferenceRequest(alias=alias, datasetTimestamp=0))

        try:
            session_id = self._get_or_create_session_id()
            request = BatchLogReferenceRequest(session_id=session_id, references=requests)
            response: BatchLogSessionReferenceResponse = (
                self._whylabs_session_api.value.batch_create_reference_profile_upload(
                    batch_log_reference_request=request, session_id=session_id
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

            individual_viewing_urls = [ref.observatory_url for ref in references]
            return UploadResult(
                viewing_url=viewing_url, result=response, individual_viewing_urls=individual_viewing_urls
            )
        except ApiException as e:
            logger.error(e)
            raise e


class LocalSession(Session):
    def __init__(self, session_config: SessionConfig) -> None:
        super().__init__(session_config)
        # the writer will default to using the session internally for credentials already

    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> Union[UploadResult, NotSupported]:
        return NotSupported()

    def upload_batch_profile(self, profile: ResultSet) -> Union[UploadResult, NotSupported]:
        return NotSupported()


class ApiKeySession(Session):
    def __init__(self, config: SessionConfig) -> None:
        from whylogs.api.usage_stats import emit_usage

        super().__init__(config)
        self.api_key = config.get_api_key()
        self.org_id = config.get_org_id()

        # Using lazy initialization to work around circular dependency issues
        self._whylabs_log_api = Lazy(partial(self.__create_log_api, config))
        emit_usage("api_key_session")

    def __create_log_api(self, config: SessionConfig) -> LogApi:
        from whylogs.api.whylabs.session.whylabs_client_cache import ClientCacheConfig

        client, _ = WhylabsClientCache.instance().get_client(
            ClientCacheConfig(
                api_key=config.get_api_key(),
            )
        )

        return LogApi(client)

    def upload_reference_profiles(self, profile_aliases: Dict[str, ResultSet]) -> Union[UploadResult, NotSupported]:
        results: List[str] = []
        for alias, profile in profile_aliases.items():
            result = profile.writer("whylabs").option(reference_profile_name=alias).write()
            results.append(*[id for status, id in result if status])

        request: GetProfileObservatoryLinkRequest = GetProfileObservatoryLinkRequest(
            reference_profile_ids=results, batch_profile_timestamps=[]
        )

        org_id = self.config.require_org_id()
        dataset_id = self.config.require_default_dataset_id()
        response: GetProfileObservatoryLinkResponse = self._whylabs_log_api.value.get_profile_observatory_link(
            dataset_id, org_id, request
        )

        return UploadResult(
            viewing_url=response.observatory_url,
            result=results,
            individual_viewing_urls=response.individual_observatory_urls,
        )

    def upload_batch_profile(self, profile: ResultSet) -> Union[UploadResult, NotSupported]:
        result = profile.writer("whylabs").write()
        utc_now = int(datetime.now(timezone.utc).timestamp() * 1000)
        timestamps: Set[int] = set()  # For generating the viewing url after upload

        for writable in profile.get_writables() or []:
            view = cast(DatasetProfileView, writable)
            if view is None:
                continue

            if view.dataset_timestamp is not None:
                timestamp = int(view.dataset_timestamp.timestamp() * 1000)
            else:
                timestamp = utc_now
            timestamps.add(timestamp)

        request: GetProfileObservatoryLinkRequest = GetProfileObservatoryLinkRequest(
            batch_profile_timestamps=list(timestamps), reference_profile_ids=[]
        )

        org_id = self.config.require_org_id()
        dataset_id = self.config.require_default_dataset_id()

        try:
            response: GetProfileObservatoryLinkResponse = self._whylabs_log_api.value.get_profile_observatory_link(
                dataset_id, org_id, request
            )
        except Exception as e:
            logger.info(f"Convenience profile links could not be generated for the sucessfully uploading profiles: {e}")

        profile_url = response.observatory_url if response else ""
        individual_urls = response.individual_observatory_urls if response else None
        return UploadResult(
            viewing_url=profile_url,
            result=result,
            individual_viewing_urls=individual_urls,
        )
