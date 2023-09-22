import time
from datetime import datetime, timezone
from typing import Optional

from whylogs.core.utils.utils import ensure_timezone


def _get_event_file_name(
    trace_id: str,
    timestamp: Optional[datetime] = None,
    column_name: Optional[str] = None,
    prefix: str = "debug_event_",
    format: str = "%Y-%m-%d_%H-%M",
    file_extension: str = "json",
) -> str:
    if not timestamp:
        time_tuple = datetime.now(timezone.utc).utctimetuple()
    else:
        ensure_timezone(timestamp)
        time_tuple = timestamp.utctimetuple()

    feature_name = f"{column_name}_" if column_name else ""

    timed_filename = f"{prefix}{feature_name}{trace_id}.{time.strftime(format, time_tuple)}.{file_extension}"
    return timed_filename
