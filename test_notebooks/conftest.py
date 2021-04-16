import os
import sys

import pytest

_MY_DIR = os.path.realpath(os.path.dirname(__file__))
# Allow import of the test utilities packages


@pytest.fixture(scope="session")
def notebooks_path():

    notebook_path = os.path.join(_MY_DIR, os.pardir, os.pardir, "notebooks")
    return os.path.abs(notebook_path)
