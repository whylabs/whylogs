import datetime
from typing import Optional


def to_utc_ms(dt: datetime.datetime) -> Optional[int]:
    if dt is None:
        return None
    return int(dt.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000.0)


def from_utc_ms(utc: Optional[int]) -> Optional[datetime.datetime]:
    if utc is None:
        return None
    return datetime.datetime.fromtimestamp(utc / 1000.0, tz=datetime.timezone.utc)
