import datetime
import os
import tempfile
from logging import getLogger

import requests
import whylabs_client
from whylabs_client.api import sessions_api
from whylabs_client.apis import SessionsApi

from whylogs.core import DatasetProfile

whylabs_api_endpoint = os.environ.get("WHYLABS_API_ENDPOINT") or "https://api.whylabsapp.com"
configuration = whylabs_client.Configuration(host=whylabs_api_endpoint)

_session_token = None
_logger = getLogger("whylogs")


def _get_whylabs_client() -> SessionsApi:
    with whylabs_client.ApiClient(configuration) as api_client:
        return sessions_api.SessionsApi(api_client)


def start_session() -> None:
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
    try:
        client = _get_whylabs_client()

        tmp_dir = tempfile.mkdtemp()
        profile_path = os.path.join(tmp_dir, "profile.bin")
        dataset_profile.write_protobuf(profile_path)

        dataset_timestamp = dataset_profile.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)
        dataset_timestamp = int(dataset_timestamp.timestamp() * 1000)

        # TODO: stop shifting dataset timestamps once we update the merger
        upload_response = client.create_dataset_profile_upload(_session_token, dataset_timestamp=dataset_timestamp - 24 * 60 * 60 * 1000)
        upload_url = upload_response.get("upload_url")

        with open(profile_path, "rb") as f:
            requests.put(upload_url, f.read())

        _logger.debug(f"Uploaded a profile for timestamp {dataset_timestamp} to WhyLabs session {_session_token}")

    except Exception:
        _logger.exception(f"Failed to upload profile for timestamp {dataset_timestamp}")


def end_session() -> str:
    try:
        global _session_token
        client = _get_whylabs_client()
        res = client.close_session(_session_token)
        _logger.debug(f"Closed session {_session_token}, returning the URL")
        return res.get("whylabs_url")

    except Exception:
        _logger.exception(f"Failed to close session {_session_token}")

    finally:
        _session_token = None
