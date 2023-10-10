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


whylogs_version = package_version("whylogs")
