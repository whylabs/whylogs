import logging
import os
from dataclasses import dataclass
from datetime import datetime
from tempfile import NamedTemporaryFile
from typing import Any, List, Optional

import requests
from whylabs_client import ApiClient, Configuration
from whylabs_client.api.sessions_api import SessionsApi
from whylabs_client.model.batch_log_reference_request import BatchLogReferenceRequest
from whylabs_client.model.log_reference_request import LogReferenceRequest

import whylogs as why
from whylogs.api.whylabs.session_manager import (
    DEFAULT_WHYLABS_HOST,
    GuestSession,
    SessionManager,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@dataclass
class DataInput:
    data: Any
    alias: Optional[str] = None
    dataset_timestamp: Optional[datetime] = None


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


def create_upload_references_url(
    api: SessionsApi,
    session_manager: SessionManager,
    references: List[DataInput],
) -> Any:
    if not isinstance(session_manager.session, GuestSession):
        raise Exception("It's not possible to log profiles to guest session on a logged session")

    references_request_list = []
    for reference in references:
        references_request_list.append(
            LogReferenceRequest(alias=reference.alias, dataset_timestamp=reference.dataset_timestamp)
        )

    response = api.batch_create_reference_profile_upload(
        session_id=session_manager.session.session_id,
        batch_log_reference_request=BatchLogReferenceRequest(references=references_request_list),
    )
    return response


def write_results(results: why.ResultSet, url_info: Any):
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


def validate_and_get_session_manager() -> SessionManager:
    assert check_if_session_is_active(), "You need an active session. Call why.init() to do so."
    assert check_if_session_is_guest(), "This method can only be used with an initialized GuestSession"

    sm = SessionManager.get_instance()
    return sm


# TODO make sure return response is actually useful


def validate_references_list(references: List[DataInput]) -> None:
    if isinstance(references, dict):
        references = [references]

    if not isinstance(references, List):
        raise ValueError("You must define a list of DataInput objects to use this method")

    for i, reference in enumerate(references):
        if not isinstance(reference, DataInput):
            references[i] = DataInput(reference)

    if len(references) > 3:
        raise ValueError("You can only set 3 reference profiles at once")


def turn_value_into_timestamp(datetime_value: Optional[datetime] = None) -> int:
    datetime_value = datetime_value or datetime.now()
    return int(1000 * datetime_value.timestamp())


def log_references(references: List[DataInput]):
    validate_references_list(references=references)

    for data_input in references:
        data_input.dataset_timestamp = turn_value_into_timestamp(datetime_value=data_input.dataset_timestamp)  # type: ignore

    sm = validate_and_get_session_manager()
    api_client = get_client(session_manager=sm)
    api = get_sessions_api(client=api_client)

    url_info = create_upload_references_url(api=api, session_manager=sm, references=references)

    responses = []

    for i, data_input in enumerate(references):
        result_set = why.log(data_input.data)
        responses.append(write_results(results=result_set, url_info=url_info.references[i]))

    return responses
