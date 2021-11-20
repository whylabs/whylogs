import datetime
import os
import tempfile
from logging import getLogger
from typing import Optional

import requests
import whylabs_client
from whylabs_client.api import sessions_api
from whylabs_client.api.log_api import LogApi
from whylabs_client.apis import SessionsApi
from whylabs_client.model.log_async_request import LogAsyncRequest

from whylogs.core import DatasetProfile

whylabs_api_endpoint = os.environ.get("WHYLABS_API_ENDPOINT") or "https://api.whylabsapp.com"
configuration = whylabs_client.Configuration(host=whylabs_api_endpoint)

_session_token = None
_logger = getLogger("whylogs")

_api_key = None
_api_log_client = None


def _get_whylabs_client() -> SessionsApi:
    with whylabs_client.ApiClient(configuration) as api_client:
        return sessions_api.SessionsApi(api_client)


def _get_or_create_log_client() -> LogApi:
    global _api_log_client, _api_key
    if _api_key is None:
        _api_key = os.environ["WHYLABS_API_KEY"]
        _logger.warning(f"Using API key ID: {_api_key[:10]}")
        config = whylabs_client.Configuration(host=whylabs_api_endpoint, api_key={"ApiKeyAuth": _api_key}, discard_unknown_keys=True)
        _api_log_client = whylabs_client.ApiClient(config)
    elif os.environ["WHYLABS_API_KEY"] is not None and os.environ["WHYLABS_API_KEY"] != _api_key:
        updated_key = os.environ["WHYLABS_API_KEY"]
        _logger.warning(f"Updating API key ID from: {_api_key[:10]} to: {updated_key[:10]}")
        _api_key = updated_key
        config = whylabs_client.Configuration(host=whylabs_api_endpoint, api_key={"ApiKeyAuth": _api_key}, discard_unknown_keys=True)
        _api_log_client = whylabs_client.ApiClient(config)

    if _api_log_client is None:
        config = whylabs_client.Configuration(host=whylabs_api_endpoint, api_key={"ApiKeyAuth": _api_key}, discard_unknown_keys=True)
        _api_log_client = whylabs_client.ApiClient(config)
    return LogApi(_api_log_client)


def start_session() -> None:
    if _api_key:
        _logger.debug("API token is present. Do not start Guest Session")
        return

    try:
        client = _get_whylabs_client()

        response = client.create_session()
        global _session_token
        _session_token = response.get("token")
        _logger.debug(f"Created a new WhyLabs session: {_session_token}")

    except Exception:
        _logger.exception("Failed to create a WhyLabs session")
        _session_token = None


def upload_profile(dataset_profile: DatasetProfile) -> None:
    tmp_dir = tempfile.mkdtemp()
    profile_path = os.path.join(tmp_dir, "profile.bin")
    dataset_profile.write_protobuf(profile_path)

    dataset_timestamp = dataset_profile.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
    dataset_timestamp = int(dataset_timestamp.timestamp() * 1000)
    if _session_token:
        _upload_guest_session(dataset_timestamp, profile_path)
    else:
        _upload_whylabs(dataset_profile, dataset_timestamp, profile_path)


def _upload_whylabs(dataset_profile, dataset_timestamp, profile_path):
    _logger.info("Upload with WhyLabs API token")
    log_api = _get_or_create_log_client()
    org_id = dataset_profile.tags.get("orgId", os.environ.get("WHYLABS_DEFAULT_ORG_ID"))
    dataset_id = dataset_profile.tags.get("datasetId", os.environ.get("WHYLABS_DEFAULT_DATASET_ID"))
    if org_id is None:
        raise EnvironmentError('Missing organization ID. Specify it via "orgId" tag or WHYLABS_DEFAULT_ORG_ID environment variable')
    if dataset_id is None:
        raise EnvironmentError('Missing dataset ID. Specify it via "datasetId" tag or WHYLABS_DEFAULT_DATASET_ID environment variable')
    try:
        with open(profile_path, "rb") as f:
            request = LogAsyncRequest(dataset_timestamp=dataset_timestamp, segment_tags=[])
            result = log_api.log_async(org_id=org_id, dataset_id=dataset_id, log_async_request=request)
            http_response = requests.put(result["upload_url"], data=f.read())
            if http_response.ok:
                _logger.info(f"Done uploading {org_id}/{dataset_id}/{dataset_timestamp} to {whylabs_api_endpoint} with API token ID: {_api_key[:10]}")
            else:
                _logger.error(
                    f"Failed to upload {org_id}/{dataset_id}/{dataset_timestamp} to "
                    + f"{whylabs_api_endpoint}: log_async returned unexpected HTTP status {http_response}"
                )
    except:  # noqa
        _logger.exception(f"Failed to upload {org_id}/{dataset_id}/{dataset_timestamp} to {whylabs_api_endpoint} with API token ID: {_api_key[:10]}")


def _upload_guest_session(dataset_timestamp: int, profile_path: str):
    client = _get_whylabs_client()

    try:
        _logger.debug("Upload via guest session")
        upload_response = client.create_dataset_profile_upload(_session_token, dataset_timestamp=dataset_timestamp)
        upload_url = upload_response.get("upload_url")

        with open(profile_path, "rb") as f:
            requests.put(upload_url, f.read())
        _logger.debug(f"Uploaded a profile for timestamp {dataset_timestamp} to WhyLabs session {_session_token}")
    except:  # noqa
        _logger.exception(f"Failed to upload profile for timestamp {dataset_timestamp}")


def end_session() -> Optional[str]:
    global _session_token
    if _session_token is None:
        _logger.info("No guest session to close")
        return None

    try:
        client = _get_whylabs_client()
        res = client.close_session(_session_token)
        _logger.debug(f"Closed session {_session_token}, returning the URL")
        return res.get("whylabs_url")
    except:  # noqa
        _logger.exception(f"Failed to close session {_session_token}")
    finally:
        _session_token = None
