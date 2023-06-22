import os
from pathlib import Path

import pytest


@pytest.fixture
def auth_path() -> Path:
    auth_path = Path(Path().home(), "whylogs_config.ini")
    yield auth_path
    os.remove(auth_path)
