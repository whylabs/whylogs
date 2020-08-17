from ._version import __version__

from .app.session import get_session, get_logger, get_or_create_session


def display_logging(level="DEBUG", root_logger=False):
    """
    Convenience utility for setting whylogs to print logs to stdout.

    Parameters
    ----------
    level : str
        Logging level
    root_logger : bool
        Redirect to the root logger.
    """
    import logging
    import sys

    HANDLER_NAME = "whylogs-display"

    if isinstance(level, str):
        level = level.upper()

    logger = logging.getLogger("whylabs.logs")
    logger.propagate = root_logger
    logger.setLevel(level)
    handlers = getattr(logger, "handlers", [])

    handler_exists = False
    for h in handlers:
        if h.name == HANDLER_NAME:
            # We already have setup a handler
            handler_exists = True

    if not handler_exists:
        handler = logging.StreamHandler(sys.stdout)
        handler.name = HANDLER_NAME
        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        )
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    # Verify
    logging.getLogger("whylabs.logs").debug(
        f"whylabs.logs logging -> stdout at level {level}"
    )
