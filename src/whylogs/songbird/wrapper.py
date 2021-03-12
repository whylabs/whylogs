from google.protobuf.message import Message
import whylabs_client
from whylabs_client.api import sessions_api
import requests

configuration = whylabs_client.Configuration( host = "https://songbird.development.whylabsdev.com" )

session_token = None

def get_songbird_client():
    with whylabs_client.ApiClient(configuration) as api_client:
        return sessions_api.SessionsApi(api_client)

def start_session():
    global session_token
    client = get_songbird_client()
    try:
        response = client.create_session()
        session_token = response.get("token")
    except Exception as e:
        print(f"Failed to create songbird session: {e}")
        session_token = None
    print(f"Started songbird session with token {session_token}")

def upload_profile(protobuf_profile: Message):
    print(f"Uploading profile via songbird wrapper. Session token {session_token}")
    client = get_songbird_client()
    try:
        songbird_response = client.create_dataset_profile_upload(session_token)
        upload_url = songbird_response.get("upload_url")
        s3_response = requests.put(upload_url, protobuf_profile.SerializeToString())
    except Exception as e:
        print(f"Failed to upload profile: {e}")

def end_session():
    global session_token
    print(f"Closing session {session_token}")
    client = get_songbird_client()
    try:
        client.close_session(session_token)
        url = f"https://observatory.development.whylabsdev.com/models?sessionToken={session_token}"
        return url
    except Exception as e:
        print(f"Failed to close session {session_token}: {e}")
    session_token = None
