import logging
import os
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Any, Optional

import requests
from whylabs_client import ApiClient, Configuration
from whylabs_client.api.sessions_api import SessionsApi
from whylabs_client.model.log_reference_request import LogReferenceRequest

import whylogs as why
from whylogs.api.whylabs.session_manager import (
    DEFAULT_WHYLABS_HOST,
    GuestSession,
    SessionManager,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def check_if_session_is_active() -> bool:
    if SessionManager.is_active():
        return True
    return False


def check_if_session_is_guest() -> bool:
    if isinstance(SessionManager.get_instance().session, GuestSession):
        return True
    return False


def get_client(session_manager: SessionManager) -> ApiClient:
    client_config = Configuration()
    client_config.host = os.getenv("WHYLABS_HOST") or DEFAULT_WHYLABS_HOST
    client_config.discard_unknown_keys = True
    client = ApiClient(client_config)
    return client


def get_sessions_api(client: ApiClient) -> SessionsApi:
    return SessionsApi(client)


def create_upload_reference_url(
    api: SessionsApi,
    session_manager: SessionManager,
    alias: Optional[str] = None,
    dataset_timestamp: Optional[datetime] = None,
) -> Any:
    if not isinstance(session_manager.session, GuestSession):
        raise Exception("It's not possible to log profiles to guest session on a logged session")

    dataset_timestamp = dataset_timestamp or datetime.now()
    dataset_timestamp_epoch = int(1000 * dataset_timestamp.timestamp())

    response = api.create_reference_profile_upload(
        session_id=session_manager.session.session_id,
        log_reference_request=LogReferenceRequest(alias=alias, dataset_timestamp=dataset_timestamp_epoch),
    )
    return response


def log_reference(data: Any, alias: Optional[str] = None, dataset_timestamp: Optional[datetime] = None):
    assert check_if_session_is_active(), "You need an active session. Call why.init() to do so."
    assert check_if_session_is_guest(), "This method can only be used with an initialized GuestSession"

    sm = SessionManager.get_instance()

    # 1. Get the API client with get_client() and assign it to `api_client`
    api_client = get_client(session_manager=sm)

    # 2. Get API from client with get_sessions_api(api_client) and assign it to `api`
    api = get_sessions_api(client=api_client)
    # 3. Create upload URL with url=create_upload_reference_url(api, alias: Optional[str] = None, dataset_timestamp: Optional[datetime]=None)
    url_info = create_upload_reference_url(
        api=api, session_manager=sm, alias=alias, dataset_timestamp=dataset_timestamp
    )
    # 4. Log incoming data with why.log(data) and assign to variable `results`
    results = why.log(data)
    # 5. Write `results` to a tempfile.NamedTemporaryFile() and assign it to `tmp_file`
    with NamedTemporaryFile() as tmp_file:
        results.view().write(file=tmp_file)
        tmp_file.flush()
        tmp_file.seek(0)

        response = requests.put(
            url_info.upload_url,
            headers={"Content-Type": "application/octet-stream"},
            data=open(tmp_file.name, "rb").read(),
        )

        if response.status_code == 200:
            logger.info(f"Upload successful! Check your profile at {url_info.observatory_url}")

        return response


# TODO make sure return response is actually useful
