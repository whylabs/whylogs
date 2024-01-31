import os

from whylogs.api.whylabs.session.session_manager import init
from whylogs.api.whylabs.session.session_types import InteractiveLogger

if __name__ == "__main__":
    os.environ["WHYLABS_FORCE_INTERACTIVE"] = "true"
    InteractiveLogger._is_notebook = True
    init()
