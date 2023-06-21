# Various common types to avoid circular dependencies
from whylogs.api.whylabs.session.notebook_check import is_notebook

_is_notebook: bool = False


# This is used to indicate that a result is either a success or a failure
class NotSupported:
    """
    Indicates that one of the session apis isn't supported for the current session type.
    """

    pass


def init_notebook_logging() -> None:
    global _is_notebook
    if is_notebook():
        _is_notebook = True


def log_if_notebook(message: str = "") -> None:
    """
    Log a message only if we're in a notebook environment.
    """
    global _is_notebook
    if _is_notebook:
        print(message)
