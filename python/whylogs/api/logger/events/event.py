import json
import logging
import time
from typing import Any, Dict, List, Optional
from uuid import uuid4

from .file_name import _get_event_file_name

diagnostic_logger = logging.getLogger(__name__)


def _extract_default_metadata(
    event: Optional[Dict[str, Any]] = None,
    *,
    name: Optional[str] = None,
    column_name: Optional[str] = None,
    trace_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
    timestamp: Optional[float] = None,
    segment_key_values: Optional[List[Dict[str, str]]] = None,
) -> Dict[str, Any]:
    if trace_id is not None:
        if not isinstance(trace_id, str):
            raise ValueError(
                f"trace_id must be a string if supplied. trace_id={trace_id} was passed with type: {type(trace_id)}."
            )
        if not trace_id:
            diagnostic_logger.warning("trace_id was supplied but empty. A new uuid will be used instead.")

    if not trace_id:
        trace_id = str(uuid4())

    if event is None:
        event = dict()
    event["whylabs.traceId"] = trace_id
    event["whylogs.creationTimestamp"] = time.time()
    if timestamp:
        event["whylogs.datasetTimestamp"] = timestamp

    if column_name:
        event["whylogs.column_name"] = column_name
    if name:
        event["whylogs.event.name"] = name
    if tags:
        event["whylogs.tags"] = set(tags)
    if segment_key_values:
        event["whylogs.segment"] = dict()
        event["whylogs.segment"]["tags"] = segment_key_values

    return event


def log_debug_event(
    debug_event: Optional[Dict[str, Any]] = None,
    *,
    name: Optional[str] = None,
    column_name: Optional[str] = None,
    trace_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
    timestamp: Optional[float] = None,
    segment_key_values: Optional[Dict[str, str]] = None,
    write_local_file: bool = False,
):
    extracted_event = _extract_default_metadata(
        event=debug_event,
        name=name,
        trace_id=trace_id,
        tags=tags,
        timestamp=timestamp,
        segment_key_values=segment_key_values,
    )

    if write_local_file:
        filename = _get_event_file_name(trace_id=trace_id, column_name=column_name, timestamp=timestamp)
        with open(filename, "w") as json_file:
            json.dump(extracted_event, json_file)

        diagnostic_logger.info(f"debug_event saved to {filename}")
    else:
        diagnostic_logger.warning(
            "debug_event not writing to WhyLabs, only local file write is supported in this build"
        )
