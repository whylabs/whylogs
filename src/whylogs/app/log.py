from whylogs.app.logger import Logger
from whylogs.app.session import _LoggerKey, get_or_create_session


def log(data: dict, dataset_name: str = "", **kwargs) -> Logger:

    session = get_or_create_session()

    logger_key = str(_LoggerKey(dataset_name=dataset_name, dataset_timestamp=session._session_time, session_timestamp=session._session_time, **kwargs))
    logger = session._loggers.get(logger_key)
    if logger is None or not logger.is_active():
        logger = Logger(
            dataset_name=dataset_name,
            dataset_timestamp=session._session_time,
            session_id=session._session_id,
            session_timestamp=session._session_time,
            **kwargs,
        )
    logger.log(data)
    session._loggers[logger_key] = logger
    return logger
