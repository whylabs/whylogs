from datetime import datetime


def to_utc_milliseconds(timestamp: datetime) -> int:
    return int(timestamp.timestamp() * 1000)
