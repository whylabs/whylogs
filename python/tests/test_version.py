from whylogs import __version__, package_version

current_version = "1.0.0"


def test_package_version() -> None:
    """Test package version calculation."""
    assert package_version() == current_version


def test_package_version_not_found() -> None:
    """Test package version calculation when package is not installed."""
    assert package_version(package="incorrect") == "incorrect is not installed."


def test_version() -> None:
    """Test package version number."""
    assert __version__ == current_version
