import atexit
import hashlib
import http.client
import json
import logging
import os
import site
import socket
import sys
import uuid
from datetime import datetime
from threading import Thread
from typing import Any, Dict
from urllib import request

import whylogs

HEAP_APPID_PROD = "2170415941"

HEAP_ENDPOINT = "https://heapanalytics.com/api/track"
HEAP_HEADERS = {"Content-Type": "application/json"}
TIMESTAMP_FORMAT = "%Y-%m-%dT%H:%M:%S.%fZ"
logger = logging.getLogger(__name__)

ANALYTICS_OPT_OUT = "WHYLOGS_NO_ANALYTICS"

# Flag to disable it internally
_TELEMETRY_DISABLED = False


_SITE_PACKAGES = site.getsitepackages()


def emit_usage(event: str) -> None:
    if _TELEMETRY_DISABLED:
        return

    t = Thread(target=_do_emit_usage, args=(event,))
    t.start()

    atexit.register(t.join)


_metadata = None
_identity = None


def _do_emit_usage(event: str) -> None:
    if os.getenv(ANALYTICS_OPT_OUT) is not None:
        logger.debug("Opted out of usage statistics. Skipping.")
        return

    logger.debug("Telemetry opted in. Emitting usage statistics")

    global _identity
    global _metadata
    if _identity is None:
        _identity = _calc_identity()
    if _metadata is None:
        _metadata = _build_metadata()

    _send_heap_event(event, _identity, _metadata)


def _calc_identity() -> str:
    try:
        hashed_computer_name = hashlib.sha512(bytes(socket.gethostname(), encoding="utf8"))
        return hashed_computer_name.hexdigest()
    except socket.timeout as exc:
        logger.debug(
            "Socket timeout when trying to get the computer name. Exception: %s",
            exc,
        )
        return uuid.uuid4().hex


def _build_metadata() -> Dict[str, Any]:
    """Hash system and project data to send to Heap."""

    project_version = whylogs.__version__
    (major, minor, macro, _, _) = sys.version_info

    metadata = {
        "project_version": project_version,
        "python_version": f"{major}.{minor}.{macro}",
        "python_version_full": sys.version,
        "terminal": _get_terminal_mode(),
        "os": sys.platform,
        "conda": ("CONDA_DEFAULT_ENV" in os.environ),
        "venv": ("VIRTUAL_ENV" in os.environ),
        "environment": _get_environment(),
    }

    # track various integrations
    integrations = {
        "numpy": _has_lib("numpy"),
        "pandas": _has_lib("pandas"),
        "mlflow": _has_lib("mlflow"),
        "dask": _has_lib("dask"),
        "ray": _has_lib("ray"),
        "airflow": _has_lib("airflow"),
        "pyspark": _has_lib("pyspark"),
        "flyte": _has_lib("flyte"),
        "kafka": _has_lib("kafka"),
    }
    for k in list(integrations.keys()):
        if integrations.get(k) is False:
            integrations.pop(k)

    # add integration metadata
    metadata.update(integrations)
    return metadata


def _get_heap_app_id() -> str:
    """
    Get the Heap App ID to send the data to.
    This will be the development ID if it's set as an
    environment variable, otherwise it will be the production ID.
    """
    return os.environ.get("HEAP_APPID_DEV", HEAP_APPID_PROD)


def _send_heap_event(event_name: str, identity: str, properties: Dict[str, Any] = None) -> None:
    data = {
        "app_id": _get_heap_app_id(),
        "identity": identity,
        "event": event_name,
        "timestamp": datetime.utcnow().strftime(TIMESTAMP_FORMAT),
        "properties": properties or {},
    }
    global _TELEMETRY_DISABLED
    json_data = json.dumps(data).encode()
    req = request.Request(HEAP_ENDPOINT, data=json_data, method="POST")
    req.add_header("Content-Type", "application/json")
    resp: http.client.HTTPResponse = None  # type: ignore
    try:
        resp = request.urlopen(req, timeout=3)
        if resp.status != 200:
            logger.warning("Unable to send usage stats. Disabling stats collection.")
            _TELEMETRY_DISABLED = True
        logger.debug("Response: %s", resp.read())
    except:  # noqa
        logger.warning("Connection error. Skip stats collection.")
        _TELEMETRY_DISABLED = True

    finally:
        if resp is not None:
            resp.close()


def _get_terminal_mode() -> str:
    try:
        from IPython.core.getipython import get_ipython  # type: ignore

        ipython = get_ipython()
        if ipython is not None:
            return ipython.__class__.__name__
    except:  # noqa
        pass

    if hasattr(sys, "ps1"):
        return "shell"
    else:
        return "headless"


def _get_environment() -> str:
    environments_dict = {
        "GITHUB_ACTION": "github_action",
        "GITLAB_CI": "gitlab_ci",
        "BINDER_PORT": "binder",
        "PYCHARM_HOSTED": "pycharm",
        "SM_CURRENT_HOST": "sagemaker",
        "DATABRICKS_RUNTIME_VERSION": "databricks",
        "COLAB_GPU": "colab",
        "KAGGLE_KERNEL_RUN_TYPE": "kaggle",
        "DEEPNOTE_PROJECT_ID": "deepnote",
    }

    for key, value in environments_dict.items():
        if key in os.environ:
            return value
    return "unknown"


def _has_lib(lib_name: str) -> bool:
    try:
        for p in _SITE_PACKAGES:
            if os.path.exists(os.path.join(p, lib_name)):
                return True
    except:  # noqa
        pass

    return False
