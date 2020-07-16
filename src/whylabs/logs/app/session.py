"""
"""
from logging import getLogger as _getLogger

from whylabs.logs.app import config as app_config
from whylabs.logs.app.logger import Logger


class Session:
    """
    Parameters
    ----------
    activate : bool (default=True)
        Activate the returned session.  If False, the **kwargs must be empty!
    **kwargs :
        Session configuration, passed to `whylabs.logs.app.config.load_config`
    """
    def __init__(self, activate=True, **kwargs):
        self.reset()
        if activate:
            self.activate()
        else:
            if len(kwargs) > 0:
                raise ValueError(
                    "Cannot parse session kwargs with activate=False")

    def reset(self):
        self._active = False
        self.config = None
        if hasattr(self, 'logger'):
            self.logger.reset()
        else:
            self.logger = Logger()

    def activate(self, **kwargs):
        """
        Load config and activate the session
        """
        self._init_config = kwargs
        self.config = app_config.load_config(**kwargs)
        self.logger.set_session_config(self.config)
        self._active = True

    def is_active(self):
        return self._active


# Create a global session
_session = Session(activate=False)


def reset_session():
    """
    Reset and inactivate the logging session.
    """
    global _session
    _session.reset()


def get_or_create_session(**kwargs):
    """
    Retrieve the current active session.  If no active session is found,
    create the session.

    Parameters
    ----------
    kwargs:
        Session configuration, passed to the session.  These are ignored
        if an active session is already found.

    Returns
    -------
    session : Session
        The active session
    """
    global _session
    if _session.is_active():
        _getLogger(__name__).debug(
            'Active session found, ignoring session kwargs')
    else:
        _session.activate(**kwargs)
    return _session


def get_session():
    """
    Retrieve the logging session without altering or activating it.
    """
    return _session


def get_logger():
    """
    Retrieve the logger.
    """
    return _session.logger
