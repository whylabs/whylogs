"""
whylogs_v1
---
"""
from importlib.metadata import version


def package_version(package: str = __package__) -> str:
    """Calculate version number based on pyproject.toml"""
    try:
        return version(package)
    except Exception:
        return "Package not found."


__version__ = package_version()
