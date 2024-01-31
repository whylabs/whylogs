import json
import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from whylabs_client import ApiClient
from whylabs_client.api.debug_events_api import DebugEventsApi

from whylogs.api.whylabs.session.session_manager import default_init
from whylogs.api.whylabs.session.whylabs_client_cache import (
    ClientCacheConfig,
    WhylabsClientCache,
)
from whylogs.core.metadata import _populate_common_profile_metadata
from whylogs.core.utils import ensure_timezone
from whylogs.core.utils.timestamp_calculations import to_utc_milliseconds

from .file_name import _get_event_file_name

diagnostic_logger = logging.getLogger(__name__)


class DebugClient:
    def __init__(
        self,
        api_client: Optional[ApiClient] = None,
        org_id: Optional[str] = None,
        dataset_id: Optional[str] = None,
        ssl_ca_cert: Optional[str] = None,
    ):
        if api_client is None:
            session = default_init()
            config = session.config
            self._cache_config = ClientCacheConfig(
                ssl_ca_cert=ssl_ca_cert,
                api_key=config.require_api_key(),
            )
            cache = WhylabsClientCache.instance()
            self._api_client, _ = cache.get_client(self._cache_config)
            self._org_id = config.require_org_id()
            self._dataset_id = config.require_default_dataset_id()
        else:
            self._api_client = api_client
            self._org_id = org_id
            self._dataset_id = dataset_id
        self._debug_events_api = DebugEventsApi(self._api_client)

    def log_debug_event(
        self,
        debug_event: Optional[Dict[str, Any]] = None,
        *,
        trace_id: str,
        tags: Optional[List[str]] = None,
        dataset_timestamp: Optional[datetime] = None,
        segment_key_values: Optional[Dict[str, str]] = None,
    ):
        from whylabs_client.model.debug_event import DebugEvent
        from whylabs_client.model.segment import Segment
        from whylabs_client.model.segment_tag import SegmentTag

        if segment_key_values is not None and not isinstance(segment_key_values, Dict):
            diagnostic_logger.warning(
                f"segment_key_values should be a dictionary of key value pairs but was passed as type: {type(segment_key_values)}"
            )

        segment_tags = list()
        now_ms = to_utc_milliseconds(datetime.now(timezone.utc))
        if segment_key_values is not None:
            for segment_key, segment_value in segment_key_values.items():
                if not isinstance(segment_value, str):
                    diagnostic_logger.info(
                        "debug event segment_key_values has a non-string value for "
                        f"{segment_key}: {segment_value}, converting to str"
                    )
                    segment_value = str(segment_value)
                segment_tags.append(SegmentTag(key=segment_key, value=segment_value))
        debug_segment = Segment(tags=segment_tags)
        if dataset_timestamp is not None:
            ensure_timezone(dataset_timestamp)
        dataset_timestamp_in_ms = to_utc_milliseconds(dataset_timestamp) if dataset_timestamp else now_ms
        whylabs_debug_event = DebugEvent(
            content=json.dumps(debug_event),
            trace_id=trace_id,
            tags=tags,
            segment=debug_segment,
            dataset_timestamp=dataset_timestamp_in_ms,
            creation_timestamp=now_ms,
        )
        # TODO: retry
        try:
            status = self._debug_events_api.log_debug_event(
                org_id=self._org_id, dataset_id=self._dataset_id, debug_event=whylabs_debug_event
            )
            diagnostic_logger.info(
                f"Wrote debug event to WhyLabs with trace_id: {trace_id} and content {debug_event}"
                f" status is ({status})"
            )
            return (True, trace_id)
        except Exception as e:
            diagnostic_logger.exception(
                f"Failed to write debug event to {self._org_id}/{self._dataset_id} for trace_id: ("
                f"{trace_id}) error was: {e}"
            )
            return (False, f"Failed writing event with trace_id: ({trace_id}) error was: {e}")


def log_debug_event(
    debug_event: Optional[Dict[str, Any]] = None,
    *,
    trace_id: str,
    name: Optional[str] = None,
    tags: Optional[List[str]] = None,
    dataset_timestamp: Optional[datetime] = None,
    segment_key_values: Optional[Dict[str, str]] = None,
    write_local_file: bool = False,
):
    default_results = (False, f"Error writing debug event: {debug_event}")
    file_results = None
    api_client_results = None
    if write_local_file:
        filename = _get_event_file_name(trace_id=trace_id, timestamp=dataset_timestamp)
        extracted_event = _populate_common_profile_metadata(
            metadata=debug_event, name=name, trace_id=trace_id, tags=tags, timestamp=dataset_timestamp
        )
        with open(filename, "w") as json_file:
            json.dump(extracted_event, json_file)

        diagnostic_logger.info(f"debug_event saved locally to {filename}")
        file_results = (True, filename)
    else:
        debug_writer = DebugClient()
        api_client_results = debug_writer.log_debug_event(
            debug_event=debug_event,
            trace_id=trace_id,
            tags=tags,
            dataset_timestamp=dataset_timestamp,
            segment_key_values=segment_key_values,
        )

    results = api_client_results or file_results or default_results
    return results
