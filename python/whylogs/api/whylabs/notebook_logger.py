import logging

from whylogs.api.whylabs.notebook_check import is_notebook

notebook_logger_name = "ai.whylabs.notebook_logger"
notebook_logger = logging.getLogger(notebook_logger_name)

_enabled: bool = False


def notebook_log(message: str = "") -> None:
    global _enabled
    if _enabled:
        print(message)


def init_notebook_logging() -> None:
    global _enabled
    if is_notebook():
        _enabled = True
