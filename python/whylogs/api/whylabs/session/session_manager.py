import logging
from typing import Optional

from whylogs.api.whylabs.session.config import INIT_DOCS, InitConfig, SessionConfig
from whylogs.api.whylabs.session.session import (
    ApiKeySession,
    GuestSession,
    LocalSession,
    Session,
)
from whylogs.api.whylabs.session.session_types import InteractiveLogger as il
from whylogs.api.whylabs.session.session_types import SessionType

logger = logging.getLogger(__name__)


class SessionManager:
    _instance: Optional["SessionManager"] = None
    session: Session

    def __init__(
        self,
        config: SessionConfig,
    ):
        session_type = config.get_session_type()
        if session_type == SessionType.LOCAL:
            self.session = LocalSession(config)
        elif session_type == SessionType.WHYLABS_ANONYMOUS:
            self.session = GuestSession(config)
        elif session_type == SessionType.WHYLABS:
            self.session = ApiKeySession(config)
        else:
            raise ValueError(f"Unknown session type: {session_type}")

    @staticmethod
    def init(session_config: SessionConfig) -> "SessionManager":
        if SessionManager._instance is None:
            SessionManager._instance = SessionManager(session_config)
        else:
            logger.debug("SessionManager is already initialized. Ignoring call to init()")

        return SessionManager._instance

    @staticmethod
    def reset() -> None:
        SessionManager._instance = None

    @staticmethod
    def get_instance() -> Optional["SessionManager"]:
        return SessionManager._instance

    @staticmethod
    def is_active() -> bool:
        return SessionManager.get_instance() is not None


def init(
    reinit: bool = False,
    allow_anonymous: bool = True,
    allow_local: bool = False,
    whylabs_api_key: Optional[str] = None,
    default_dataset_id: Optional[str] = None,
    config_path: Optional[str] = None,
    **kwargs: bool,
) -> Session:
    """
    Set up authentication for this whylogs logging session. There are three modes that you can authentiate in.

    1. WHYLABS: Data is sent to WhyLabs and is associated with a specific WhyLabs account. You can get a WhyLabs api
        key from the WhyLabs Settings page after logging in.
    2. WHYLABS_ANONYMOUS: Data is sent to WhyLabs, but no authentication happens and no WhyLabs account is required.
        Sessions can be claimed into an account later on the WhyLabs website.
    3. LOCAL: No authentication. No data is automatically sent anywhere. Use this if you want to explore profiles
        locally or manually upload them somewhere.

    Typically, you should only have to put `why.init()` with no arguments at the start of your application/notebook/script.
    The arguments allow for some customization of the logic that determines the session type. Here is the priority order:

    - If there is an api key directly supplied to init, then use it and authenticate session as WHYLABS.
    - If there is an api key in the environment variable WHYLABS_API_KEY, then use it and authenticate session as WHYLABS.
    - If there is an api key in the whylogs config file, then use it and authenticate session as WHYLABS.
    - If we're in an interractive environment (notebook, colab, etc.) then prompt the user to pick a method explicitly.
        The options are determined by the allow* argument values to init().
    - If allow_anonymous is True, then authenticate session as WHYLABS_ANONYMOUS.
    - If allow_local is True, then authenticate session as LOCAL.

    Args:
        session_type: Deprecated, use allow_anonymous and allow_local instead
        reinit: Normally, init() is idempotent, so you can run it over and over again in a notebook without any issues, for example.
            If reinit=True then it will run the initialization logic again, so you can switch authentication methods without restarting.
        allow_anonymous: If True, then the user will be able to choose WHYLABS_ANONYMOUS if no other authentication method is found.
        allow_local: If True, then the user will be able to choose LOCAL if no other authentication method is found.
        whylabs_api_key: A WhyLabs api key to use for uploading profiles. There are other ways that you can set an api key that don't
            require direclty embedding it in code, like setting WHYLABS_API_KEY env variable or supplying the api key interractively
            via the init() prompt in a notebook.
        default_dataset_id: The default dataset id to use for uploading profiles. This is only used if the session is authenticated.
            This is a convenience argument so that you don't have to supply the dataset id every time you upload a profile if
            you're only using a single dataset id.

    """
    if reinit:
        SessionManager.reset()

    manager: Optional[SessionManager] = SessionManager._instance  # type: ignore
    if manager is not None:
        return manager.session

    session_config = SessionConfig(
        InitConfig(
            allow_anonymous=allow_anonymous,
            allow_local=allow_local,
            whylabs_api_key=whylabs_api_key,
            default_dataset_id=default_dataset_id,
            config_path=config_path,
            force_local=kwargs.get("force_local", False),
        )
    )

    try:
        manager = SessionManager.init(session_config)
        session_config.notify_session_type()
        return manager.session
    except PermissionError as e:
        # TODO PR this implies that we need disk access to work correctly, but isn't that already the case
        # because we write profilfes to disk as tmp files?
        logger.warning("Could not create or read configuration file for session. Profiles won't be uploaded.", e)
        raise e
    except Exception as e:
        logger.warning("Could not initialize session", e)
        raise e


def get_current_session() -> Optional[Session]:
    manager = SessionManager.get_instance()
    if manager is not None:
        return manager.session

    il.warning_once(
        f"No session found. Call whylogs.init() to initialize a session and authenticate. See {INIT_DOCS} for more information.",
        logger.warning,
    )

    return None


def default_init() -> Session:
    """
    For internal use. This initializes a default session for the user if they don't call why.init() themselves.
    This will behave as though they called why.init() with no arguments and print out a warning with a link to the docs.
    """
    manager = SessionManager.get_instance()
    if manager is None:
        il.warning_once("Initializing default session because no session was found.", logger.warning)

        # To be safe, don't allow default session to be anonymous if this is happening as a side effect
        # that users don't know about.
        return init(allow_anonymous=False, allow_local=True, force_local=True)
    else:
        return manager.session
