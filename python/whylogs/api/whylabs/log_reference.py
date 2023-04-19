import os
from datetime import datetime

# from tempfile import NamedTemporaryFile
from typing import Any, Optional

from whylabs_client import ApiClient, Configuration
from whylabs_client.api.sessions_api import SessionsApi
from whylabs_client.model.log_reference_request import LogReferenceRequest

from whylogs.api.whylabs.session_manager import (
    DEFAULT_WHYLABS_HOST,
    GuestSession,
    SessionManager,
    UserSession,
)

# from whylogs.api.writer.writer import Writable
# from whylogs.core import DatasetProfile, DatasetProfileView, ResultSet


def check_if_session_is_active() -> bool:
    if SessionManager.is_active():
        return True
    else:
        return False


def get_client(session_manager: SessionManager) -> ApiClient:
    client_config = Configuration()
    client_config.host = os.getenv("WHYLABS_HOST") or DEFAULT_WHYLABS_HOST

    if isinstance(session_manager, UserSession):
        client_config.api_key = {"ApiKeyAuth": session_manager.session.api_key}
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

    response = api.create_reference_profile_upload(
        session_id=session_manager.session.session_id,
        log_reference_request=LogReferenceRequest(alias=alias, dataset_timestamp=dataset_timestamp or datetime.now()),
    )
    return response


# def do_upload(file: Writable, url: str):
#     # get api
#     result = api.upload_profile(file, url)
#     if result == 200:
#         return True, str(result)
#     else:
#         return False, str(result)


# def log_reference(file: Writable) -> Union[bool, str]:
#     view = file.view() if isinstance(file, DatasetProfile) or isinstance(file, ResultSet) else file
#     if not isinstance(view, DatasetProfileView):
#         raise Exception("This method needs either a DatasetProfile or DatasetProfileView.")

#     with NamedTemporaryFile() as tmp_file:
#         view.write(file=tmp_file)

#         tmp_file.flush()
#         tmp_file.seek(0)
#         utc_now = datetime.datetime.now(datetime.timezone.utc)
#         dataset_timestamp = view.dataset_timestamp or utc_now
#         stamp = dataset_timestamp.timestamp()
#         time_delta_seconds = utc_now.timestamp() - stamp

#         if stamp <= 0:
#             logger.error(
#                 f"Profiles should have timestamps greater than 0, but found a timestamp of {stamp}"
#                 f" and current timestamp is {utc_now.timestamp()}, this is likely an error."
#             )

#         dataset_timestamp_epoch = int(stamp * 1000)
#         response = self._do_upload(
#             dataset_timestamp=dataset_timestamp_epoch,
#             profile_file=tmp_file,
#         )
