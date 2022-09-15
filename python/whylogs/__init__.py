"""
whylogs is an open source library for logging any kind of data. With whylogs, users are able to generate summaries
of their datasets (called whylogs profiles) which they can use to:

* Track changes in their dataset
* Create data constraints to know whether their data looks they way it should
* Quickly visualize key summary statistics about their datasets

These three functionalities enable a variety of use cases for data scientists, machine learning engineers, and
 data engineers:

* Detecting data drift (and resultant ML model performance degradation)
* Data quality validation
* Exploratory data analysis via data profiling
* Tracking data for ML experiments
* And many more...
"""
from .api import (
    ResultSet,
    log,
    log_classification_metrics,
    log_regression_metrics,
    logger,
    profiling,
    read,
    reader,
    write,
)
from .api.usage_stats import emit_usage as __emit_usage_stats
from .core import DatasetProfileView
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
    ResultSet,
    DatasetProfileView,
    log,
    log_classification_metrics,
    log_regression_metrics,
    logger,
    read,
    reader,
    write,
    profiling,
    v0_to_v1_view,
    __version__,
]

__emit_usage_stats("import")
