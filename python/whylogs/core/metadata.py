import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from whylogs.context.version import whylogs_version
from whylogs.core.utils.timestamp_calculations import to_utc_milliseconds

diagnostic_logger = logging.getLogger(__name__)

WHYLABS_TRACE_ID_KEY = "whylabs.traceId"
CREATION_TIMESTAMP_KEY = "whylogs.creationTimestamp"
DATASET_TIMESTAMP_KEY = "whylogs.datasetTimestamp"
USER_TAGS_KEY = "whylogs.user.tags"
NAME_KEY = "whylogs.name"
WHYLOGS_VERSION_KEY = "whylogs.version"


def _populate_common_profile_metadata(
    metadata: Optional[Dict[str, str]] = None,
    *,
    name: Optional[str] = None,
    trace_id: Optional[str] = None,
    tags: Optional[List[str]] = None,
    timestamp: Optional[int] = None,
) -> Dict[str, Any]:
    if trace_id is not None:
        if not isinstance(trace_id, str):
            raise ValueError(
                f"trace_id must be a string if supplied. trace_id={trace_id} was passed with type: {type(trace_id)}."
            )
        if not trace_id:
            diagnostic_logger.warning("trace_id was supplied but empty. A new uuid will be used instead.")

    if metadata is None:
        metadata = dict()
    if WHYLABS_TRACE_ID_KEY not in metadata:
        if not trace_id:
            trace_id = str(uuid4())
        metadata[WHYLABS_TRACE_ID_KEY] = trace_id
    elif metadata[WHYLABS_TRACE_ID_KEY] != trace_id and trace_id is not None:
        diagnostic_logger.warning(
            f"trace_id was specified as {trace_id} but there is already a trace_id defined "
            f"in metadata[{WHYLABS_TRACE_ID_KEY}]: {metadata[WHYLABS_TRACE_ID_KEY]}"
        )

    if CREATION_TIMESTAMP_KEY not in metadata:
        metadata[CREATION_TIMESTAMP_KEY] = str(to_utc_milliseconds(datetime.now(timezone.utc)))
    if timestamp and DATASET_TIMESTAMP_KEY not in metadata:
        metadata[DATASET_TIMESTAMP_KEY] = str(timestamp)

    if name and NAME_KEY not in metadata:
        metadata[NAME_KEY] = name
    if tags and USER_TAGS_KEY not in metadata:
        metadata[USER_TAGS_KEY] = json.dumps(sorted(set(tags)))

    if WHYLOGS_VERSION_KEY not in metadata:
        metadata[WHYLOGS_VERSION_KEY] = whylogs_version

    return metadata


def _safe_merge_metadata(
    default_metadata: Optional[Dict[str, str]], incoming_metadata: Optional[Dict[str, str]]
) -> Dict[str, str]:
    if default_metadata is None:
        default_metadata = dict()
    if not incoming_metadata:
        return default_metadata

    for key in incoming_metadata:
        if not default_metadata.get(key):
            default_metadata[key] = incoming_metadata[key]
        elif incoming_metadata[key] != default_metadata[key] and incoming_metadata[key]:
            diagnostic_logger.warning(
                f"metadata collision on key {key}. Current value is {default_metadata[key]} so ignoring attempt to set to {incoming_metadata[key]}"
            )
    return default_metadata
