"""
Functions for interacting with timestamps and datetime objects
"""
import datetime
from typing import Optional


def to_utc_ms(dt: datetime.datetime) -> Optional[int]:
    """
    Convert a datetime object to UTC epoch milliseconds

    Returns
    -------
    timstamp_ms : int
        Timestamp
    """
    if dt is None:
        return None
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000.0)


def from_utc_ms(utc: Optional[int]) -> Optional[datetime.datetime]:
    """
    Convert a UTC epoch milliseconds timestamp to a datetime object

    Parameters
    ----------
    utc : int
        Timestamp

    Returns
    -------
    dt : datetime.datetime
        Datetime object
    """
    if utc is None:
        return None
    return datetime.datetime.fromtimestamp(utc / 1000.0, tz=datetime.timezone.utc)
