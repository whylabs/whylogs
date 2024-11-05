import os

os.environ["TELEMETRY_DEV"] = "1"  # noqa: E402

from whylogs import __version__, log, package_version  # noqa

"""
This is here to verify that the produced wheel includes
all the necessary dependencies. This is excersized by
the CI workflow and does not use pytest because it is
intended to test the wheel in a production environment,
not a development environment.
"""

current_version = "1.6.1"


def test_package_version() -> None:
    """Test package version calculation."""
    assert package_version() == current_version, f"{package_version()}!={current_version}"


def test_package_version_not_found() -> None:
    """Test package version calculation when package is not installed."""
    assert package_version(package="incorrect") == "incorrect is not installed."


def test_version() -> None:
    """Test package version number."""
    assert __version__ == current_version


def test_basic_log() -> None:
    """test basic log scenario on row data."""
    column_name = "a_column_name"
    results = log(row={column_name: 3})
    assert results is not None
    column_profile_dictionary = results.view().get_columns()
    assert column_name in column_profile_dictionary


test_package_version()
test_package_version_not_found()
test_version()
test_basic_log()
