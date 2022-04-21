"""
whylogs
---
"""
from .api import log, profiling, read, write
from .migration.converters import v0_to_v1_view


def package_version(package: str = __package__) -> str:
    """Calculate version number based on pyproject.toml"""
    try:
        from importlib import metadata  # type: ignore
    except ImportError:
        # Python < 3.8
        import importlib_metadata as metadata  # type: ignore

    try:
        version = metadata.version(package)
    except metadata.PackageNotFoundError:  # type: ignore
        version = f"{package} is not installed."

    return version


__version__ = package_version()

__ALL__ = [
    log,
    read,
    write,
    profiling,
    v0_to_v1_view,
]
