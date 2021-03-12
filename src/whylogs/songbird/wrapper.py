from google.protobuf.message import Message

session_token = None

def start_session():
    global session_token
    session_token = "session-1337"
    print(f"Started songbird session with token {session_token}")

def upload_profile(protobuf_profile: Message):
    print(f"Uploading profile via songbird wrapper. Session token {session_token}")

def end_session():
    global session_token
    print(f"Closing session {session_token}")
    session_token = None
