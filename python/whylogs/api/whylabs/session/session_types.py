# Various common types to avoid circular dependencies
from typing import Optional

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


def log_if_notebook_wait_n_seconds(delay: Optional[int] = None) -> None:
    """
    Log a message only if we're in a notebook environment.
    """
    global _is_notebook
    if _is_notebook:
        import time

        if delay is None:
            delay = 5

        try:
            from tqdm import tqdm
            # Use tqdm to create the progress bar
            for _ in tqdm(range(delay), desc="Progress", unit="s", unit_divisor=1, leave=True):
                time.sleep(1)  # Wait for the specified increment duration

        except Exception:
            for _ in range(delay):  # Adjust the range to control the number of periods
                print(".", end="")
                time.sleep(1)  # Add a delay if desired
        print()
