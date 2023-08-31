import time
from typing import Optional


def _get_event_file_name(
    trace_id: str,
    timestamp: Optional[float] = None,
    column_name: Optional[str] = None,
    prefix: str = "debug_event_",
    format: str = "%Y-%m-%d_%H-%M",
    file_extension: str = "json",
) -> str:
    if not timestamp:
        time_tuple = time.gmtime(time.time())
    else:
        time_tuple = time.gmtime(timestamp)

    feature_name = f"{column_name}_" if column_name else ""

    timed_filename = f"{prefix}{feature_name}{trace_id}.{time.strftime(format, time_tuple)}.{file_extension}"
    return timed_filename
