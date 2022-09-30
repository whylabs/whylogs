import os

os.environ["TELEMETRY_DEV"] = "1"  # noqa: E402

from whylogs import __version__, package_version  # noqa

"""
This is here to verify that the produced wheel includes
all the necessary dependencies. This is excersized by
the CI workflow and does not use pytest because it is
intended to test the wheel in a production environment,
not a development environment.
"""

current_version = "1.1.5"


def test_package_version() -> None:
    """Test package version calculation."""
    assert package_version() == current_version


def test_package_version_not_found() -> None:
    """Test package version calculation when package is not installed."""
    assert package_version(package="incorrect") == "incorrect is not installed."


def test_version() -> None:
    """Test package version number."""
    assert __version__ == current_version


test_package_version()
test_package_version_not_found()
test_version()
