# Various common types to avoid circular dependencies
import os
from dataclasses import dataclass
from enum import Enum
from typing import Callable, Optional, Set, Union

from whylogs.api.whylabs.session.notebook_check import is_interractive


# This is used to indicate that a result is either a success or a failure
class NotSupported:
    """
    Indicates that one of the session apis isn't supported for the current session type.
    """


LogFunction = Callable[[str], None]


class InteractiveLogger:
    """
    Logger for interactive environments.
    """

    __warnings: Set[int] = set()
    _is_notebook: bool = False

    @staticmethod
    def init_notebook_logging() -> None:
        if is_interractive():
            InteractiveLogger._is_notebook = True

    @staticmethod
    def __should_log(ignore_suppress: bool = False) -> bool:
        """
        Returns true if we should log, false otherwise.
        """
        if ignore_suppress:
            return InteractiveLogger._is_notebook
        else:
            return not os.environ.get("WHYLOGS_SUPPRESS_LOG_OUTPUT") and InteractiveLogger._is_notebook

    @staticmethod
    def message(message: str = "", log_fn: Optional[LogFunction] = None, ignore_suppress: bool = False) -> None:
        """
        Log a message only if we're in a notebook environment.

        Args:
            message: The message to log
            log_fn: A function to log to instead of printing if we're not in a notebook.
            ignore_suppress: If true, will log even if WHYLOGS_SUPPRESS_LOG_OUTPUT is set. It still needs
                to be in a notebook though or it won't show.
        """
        if InteractiveLogger.__should_log(ignore_suppress=ignore_suppress):
            print(message)
        elif log_fn is not None:
            log_fn(message)

    @staticmethod
    def option(message: str, ignore_suppress: bool = False) -> None:
        """
        Log an option line, which is anything that has multiple related lines in a row like
        multiple choices or a list things.
        """
        InteractiveLogger.message(f" ⤷ {message}", ignore_suppress=ignore_suppress)

    @staticmethod
    def inspect(message: str) -> None:
        """
        Log a message that the user is intended to interact with or inspect, like a url
        """
        InteractiveLogger.message(f"🔍 {message}")

    @staticmethod
    def question(message: str, ignore_suppress: bool = False) -> None:
        """
        Log a question.
        """
        InteractiveLogger.message(f"❓ {message}", ignore_suppress=ignore_suppress)

    @staticmethod
    def success(message: str, ignore_suppress: bool = False) -> None:
        """
        Log a success line, which has a green checkmark.
        """
        InteractiveLogger.message(f"✅ {message}", ignore_suppress=ignore_suppress)

    @staticmethod
    def failure(message: str, ignore_suppress: bool = False) -> None:
        """
        Log a failure, which has a red x.
        """
        InteractiveLogger.message(f"❌ {message}", ignore_suppress=ignore_suppress)

    @staticmethod
    def warning(message: str, log_fn: Optional[LogFunction] = None, ignore_suppress: bool = False) -> None:
        """
        Log a warning, which has a warning sign.
        """
        InteractiveLogger.message(f"⚠️ {message}", log_fn=log_fn, ignore_suppress=ignore_suppress)

    @staticmethod
    def warning_once(message: str, log_fn: Optional[LogFunction] = None, ignore_suppress: bool = False) -> None:
        """
        Like warning, but only logs once.
        """
        if not InteractiveLogger.__should_log(ignore_suppress=ignore_suppress):
            return

        if hash(message) not in InteractiveLogger.__warnings:
            InteractiveLogger.message(f"⚠️ {message}", log_fn=log_fn)
            InteractiveLogger.__warnings.add(hash(message))


InteractiveLogger.init_notebook_logging()


class SessionType(Enum):
    WHYLABS_ANONYMOUS = "whylabs_anonymous"
    WHYLABS = "whylabs"
    LOCAL = "local"


@dataclass
class ApiKeyV1:
    api_key_id: str
    full_key: str


@dataclass
class ApiKeyV2:
    api_key_id: str
    org_id: str
    full_key: str


ApiKey = Union[ApiKeyV1, ApiKeyV2]


def parse_api_key(api_key: str) -> ApiKey:
    try:
        return parse_api_key_v2(api_key)
    except Exception:
        return parse_api_key_v1(api_key)


def validate_org_id(org_id: str) -> None:
    org_id.startswith("org-")


def parse_api_key_v1(api_key: str) -> ApiKeyV1:
    """
    Parse the key id from an api key.
    V1 api keys have the format key_id.key
    """

    # split on . and get the first part
    api_key_id = api_key.split(".")[0]

    assert ":" not in api_key
    return ApiKeyV1(api_key_id, api_key)


def parse_api_key_v2(api_key: str) -> ApiKeyV2:
    """
    Parse the key id and the org id from an api key
    V2 api keys have the format key_id.key:org_id
    """

    # split on . and get the first part
    api_key_id = api_key.split(".")[0]

    # split on : and get the second part
    org_id = api_key.split(":")[1]
    return ApiKeyV2(api_key_id, org_id, api_key)
