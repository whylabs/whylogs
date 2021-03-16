import whylabs_client
import os
import requests
import tempfile
import datetime
from logging import getLogger
from google.protobuf.message import Message
from whylogs.core import DatasetProfile
from whylabs_client.api import sessions_api
from time import time

whylabs_api_endpoint = os.environ['WHYLABS_API_ENDPOINT'] or "https://api.whylabsapp.com"
whylabs_observatory_endpoint = os.environ['WHYLABS_OBSERVATORY_ENDPOINT'] or "https://hub.whylabsapp.com"
configuration = whylabs_client.Configuration(host = whylabs_api_endpoint)

_session_token = None
_logger = getLogger(__name__)

def _get_whylabs_client():
    with whylabs_client.ApiClient(configuration) as api_client:
        return sessions_api.SessionsApi(api_client)

def start_session():
    try:
        client = _get_whylabs_client()

        response = client.create_session()
        global _session_token
        _session_token = response.get("token")
        _logger.debug(f"Created a new WhyLabs session: {_session_token}")

    except Exception:
        _logger.exception("Failed to create a WhyLabs session")
        _session_token = None

def upload_profile(dataset_profile: DatasetProfile):
    try:
        client = _get_whylabs_client()

        tmp_dir = tempfile.mkdtemp()
        profile_path = os.path.join(tmp_dir, "profile.bin")
        dataset_profile.write_protobuf(profile_path)

        dataset_timestamp = dataset_profile.dataset_timestamp or datetime.datetime.now(datetime.timezone.utc)

        upload_response = client.create_dataset_profile_upload(_session_token, dataset_timestamp=dataset_profile.dataset_timestamp)
        upload_url = upload_response.get("upload_url")

        with open(profile_path, 'rb') as f:
            s3_response = requests.put(upload_url, f.read())

        _logger.debug(f"Uploaded a profile for timestamp {dataset_timestamp} to WhyLabs session {_session_token}")

    except Exception:
        _logger.exception(f"Failed to upload profile for timestamp {dataset_timestamp}")

def end_session():
    try:
        global _session_token
        client = _get_whylabs_client()
        client.close_session(_session_token)
        _logger.debug(f"Closed session {_session_token}, returning the URL")
        url = f"{whylabs_observatory_endpoint}/models/model-1/profiles?sessionToken={_session_token}"
        return url

    except Exception:
        _logger.exception(f"Failed to close session {_session_token}")

    finally:
        _session_token = None
