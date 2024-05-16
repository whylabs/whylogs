"""
Messages that the process logger can process.

These types are core types that you can send to the process logger. They are either subclasses of
TypedDict or wrappers around serialized byte versions of those dicts. Everything in here is a
TypedDict because we use orjson to deserialize them for performance reasons and that library prefers
to output everything as dictionaries.

The dataclass containers for those messages have helper methods to extract/deserialize the dicts. It's
important to not raise exceptions in those data classes because the actor does a lot of large batch processing
and exceptions would result in losing the entire batch. Instead, they return None and log errors if there is
some issue deserializing or validating.
"""
import sys

if sys.version_info >= (3, 8):
    from typing import Protocol, TypedDict  # pylint: disable=no-name-in-module
else:
    from typing_extensions import Protocol, TypedDict

import base64
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Tuple, Union, cast

from whylogs.api.logger.experimental.logger.actor.thread_rolling_logger import (
    LoggerStatus,
)

try:
    import orjson  # type: ignore
except ImportError:
    raise ImportError("Install whylogs with extra [proc] for process based logging: pip install whylogs[proc]")
import logging
import uuid

import numpy as np
import pandas as pd

from whylogs.api.logger.experimental.logger.actor.time_util import (
    TimeGranularity,
    truncate_time_ms,
)

DataTypes = Union[str, int, float, bool, List[float], List[int], List[str]]

_logger = logging.getLogger("profile_actor_messages")


class DataDict(TypedDict):
    columns: List[str]
    data: List[List[DataTypes]]


def data_dict_from_pandas(df: pd.DataFrame) -> DataDict:
    multiple = df.to_dict(orient="split")
    del multiple["index"]  # get rid of this to min/max payload size
    return {
        "columns": multiple["columns"],
        "data": multiple["data"],
    }


class LogRequestDict(TypedDict):
    datasetId: str
    timestamp: Optional[int]
    multiple: DataDict


class LogEmbeddingRequestDict(TypedDict):
    datasetId: str
    timestamp: Optional[int]
    embeddings: Dict[str, List[DataTypes]]


class PubSubMessage(TypedDict):
    attributes: Dict[str, str]
    data: str
    message_id: str
    publish_time: str


class PubSubDict(TypedDict):
    subscription: str
    message: PubSubMessage
    log_request: LogRequestDict


class DebugMessage:
    pass


class FlushMessage:
    pass


@dataclass
class ProcessLoggerStatus:
    statuses: Dict[str, LoggerStatus] = field(default_factory=dict)


@dataclass
class ProcessStatusMessage:
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    timeout: float = 1.0
    sync: bool = True


@dataclass
class LogMessage:
    request_time: int
    log: LogRequestDict
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sync: bool = False


class SyncMessage(Protocol):
    """
    A message can be sent synchronously if it has an id and it has a sync flag set to True.
    It doesnt magically make the message synchronous, but allows us to create a synchronous
    convenience method for that message type. See log and status on the ProcessRollingLogger.
    """

    id: str
    sync: bool


@dataclass
class RawLogMessage:
    request: bytes
    """
    Bytes that represent json stringified LogRequestDict
    """
    request_time: int
    id: str = field(default_factory=lambda: str(uuid.uuid4()))
    sync: bool = False

    def __post_init__(self) -> None:
        if self.id == "None":
            self.id = str(uuid.uuid4())

    def to_log_request_dict(self) -> Optional[LogRequestDict]:
        d: LogRequestDict = orjson.loads(self.request)
        if "timestamp" not in d or d["timestamp"] is None:
            d["timestamp"] = self.request_time

        if "datasetId" not in d or d["datasetId"] is None:  # type: ignore
            _logger.error(f"Request missing dataset id {d}")
            return None

        if "multiple" not in d or d["multiple"] is None:  # type: ignore
            _logger.error(f"Request has no 'multiple' field {d}")
            return None

        return d


def get_columns(request: Union[LogRequestDict, LogEmbeddingRequestDict]) -> List[str]:
    maybe_request = cast(LogRequestDict, request)
    if "multiple" in maybe_request and maybe_request["multiple"] is not None:  # type: ignore
        return maybe_request["multiple"]["columns"]

    maybe_embedding = cast(LogEmbeddingRequestDict, request)
    embeddings = maybe_embedding["embeddings"]
    if embeddings is not None:  # type: ignore
        return list(embeddings.keys())

    raise Exception(f"Don't know how to get column names for request {request}.")


@dataclass
class RawPubSubMessage:
    request: bytes
    request_time: int

    def to_pubsub_message(self) -> Optional[PubSubDict]:
        data: PubSubDict = orjson.loads(self.request)
        log_request_dict_bytes = _decode_pubsub_data(data)

        if log_request_dict_bytes is None:
            return None

        log_message = RawLogMessage(request=log_request_dict_bytes, request_time=self.request_time)
        log_request = log_message.to_log_request_dict()
        if log_request is None:
            return None

        data["log_request"] = log_request
        return data


class PubSubEmbeddingDict(TypedDict):
    subscription: str
    message: PubSubMessage
    log_embedding_request: LogEmbeddingRequestDict


def _decode_pubsub_data(data: Union[PubSubEmbeddingDict, PubSubDict]) -> Optional[bytes]:
    if "message" not in data or data["message"] is None:  # type: ignore
        _logger.error(f"Request missing message field {data}")
        return None

    message = data["message"]
    encoded_data = message["data"]
    return base64.b64decode(encoded_data)


@dataclass
class RawPubSubEmbeddingMessage:
    request: bytes
    request_time: int

    def to_pubsub_embedding_message(self) -> Optional[PubSubEmbeddingDict]:
        data: PubSubEmbeddingDict = orjson.loads(self.request)
        log_request_dict_bytes = _decode_pubsub_data(data)

        if log_request_dict_bytes is None:
            return None

        log_message = RawLogEmbeddingsMessage(request=log_request_dict_bytes, request_time=self.request_time)
        log_request = log_message.to_log_embeddings_request_dict()
        if log_request is None:
            return None
        data["log_embedding_request"] = log_request
        return data


@dataclass
class RawLogEmbeddingsMessage:
    request: bytes
    request_time: int

    def to_log_embeddings_request_dict(self) -> Optional[LogEmbeddingRequestDict]:
        d: LogEmbeddingRequestDict = orjson.loads(self.request)
        if "timestamp" not in d or d["timestamp"] is None:
            d["timestamp"] = self.request_time

        if "datasetId" not in d or d["datasetId"] is None:  # type: ignore
            _logger.error(f"Request missing dataset id {d}")
            return None

        if "embeddings" not in d or d["embeddings"] is None:  # type: ignore
            _logger.error(f"Request has no embeddings field {d}")
            return None

        if not isinstance(d["embeddings"], dict):  # type: ignore
            # TODO test recovering from errors like this. It seems to brick the container
            _logger.error(
                f'Expected a dictionary format for embeddings of the form {{"column_name": "embedding_2d_list"}}. Got {self.request}'
            )
            return None

        return d


def log_dict_to_data_frame(request: LogRequestDict) -> Tuple[pd.DataFrame, int]:
    df = pd.DataFrame(request["multiple"]["data"], columns=request["multiple"]["columns"])
    return df, len(df)


def log_dict_to_embedding_matrix(request: LogEmbeddingRequestDict) -> Tuple[Dict[str, "np.ndarray[Any, Any]"], int]:
    row: Dict[str, np.ndarray[Any, Any]] = {}
    row_count = 0
    for col, embeddings in request["embeddings"].items():
        row[col] = np.array(embeddings)  # type: ignore
        row_count += len(embeddings)

    return row, row_count


def reduce_log_requests(acc: LogRequestDict, cur: LogRequestDict) -> LogRequestDict:
    """
    Reduce requests, assuming that each request has the same columns.
    That assumption should be enforced before this is used by grouping by set of columns.
    """
    acc["multiple"]["data"].extend(cur["multiple"]["data"])
    return acc


def reduce_embeddings_request(acc: LogEmbeddingRequestDict, cur: LogEmbeddingRequestDict) -> LogEmbeddingRequestDict:
    for col, embeddings in cur["embeddings"].items():
        if col not in acc["embeddings"]:
            acc["embeddings"][col] = []

        acc["embeddings"][col].extend(embeddings)

    return acc


def determine_dataset_timestamp(
    cadence: TimeGranularity, request: Union[LogRequestDict, LogEmbeddingRequestDict]
) -> Optional[int]:
    ts = request["timestamp"]
    if ts is None:
        return None

    return truncate_time_ms(ts, cadence)
