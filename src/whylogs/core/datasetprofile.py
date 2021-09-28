"""
Defines the primary interface class for tracking dataset statistics.
"""
import datetime
import io
import logging
from typing import Dict, List, Mapping, Optional, Union
from uuid import uuid4

import numpy as np
import pandas as pd
from google.protobuf.internal.decoder import _DecodeVarint32
from google.protobuf.internal.encoder import _VarintBytes
from smart_open import open

from whylogs.core import ColumnProfile
from whylogs.core.flatten_datasetprofile import flatten_summary
from whylogs.core.model_profile import ModelProfile
from whylogs.core.statistics.constraints import DatasetConstraints, SummaryConstraints
from whylogs.proto import (
    ColumnsChunkSegment,
    DatasetMetadataSegment,
    DatasetProfileMessage,
    DatasetProperties,
    DatasetSummary,
    MessageSegment,
    ModelType,
)
from whylogs.util import time
from whylogs.util.time import from_utc_ms, to_utc_ms

SCHEMA_MAJOR_VERSION = 1
SCHEMA_MINOR_VERSION = 2

logger = logging.getLogger(__name__)
# Optional import for cudf
try:
    # noinspection PyUnresolvedReferences
    from cudf.core.dataframe import DataFrame as cudfDataFrame
except ImportError as e:
    logger.debug(str(e))
    logger.debug("Failed to import CudaDataFrame. Install cudf for CUDA support")
    cudfDataFrame = None


COLUMN_CHUNK_MAX_LEN_IN_BYTES = int(1e6) - 10  #: Used for chunking serialized dataset profile messages


class DatasetProfile:
    """
    Statistics tracking for a dataset.

    A dataset refers to a collection of columns.

    Parameters
    ----------
    name: str
        A human readable name for the dataset profile. Could be model name.
        This is stored under "name" tag
    dataset_timestamp: datetime.datetime
        The timestamp associated with the data (i.e. batch run). Optional.
    session_timestamp : datetime.datetime
        Timestamp of the dataset
    columns : dict
        Dictionary lookup of `ColumnProfile`s
    tags : dict
        A dictionary of key->value. Can be used upstream for aggregating data. Tags must match when merging
        with another dataset profile object.
    metadata: dict
        Metadata that can store arbitrary string mapping. Metadata is not used when aggregating data
        and can be dropped when merging with another dataset profile object.
    session_id : str
        The unique session ID run. Should be a UUID.
    constraints: DatasetConstraints
        Static assertions to be applied to tracked numeric data and profile summaries.
    """

    def __init__(
        self,
        name: str,
        dataset_timestamp: datetime.datetime = None,
        session_timestamp: datetime.datetime = None,
        columns: dict = None,
        tags: Dict[str, str] = None,
        metadata: Dict[str, str] = None,
        session_id: str = None,
        model_profile: ModelProfile = None,
        constraints: DatasetConstraints = None,
    ):
        # Default values
        if columns is None:
            columns = {}
        if tags is None:
            tags = {}
        if metadata is None:
            metadata = {}
        if session_id is None:
            session_id = uuid4().hex

        self.session_id = session_id
        self.session_timestamp = session_timestamp
        self.dataset_timestamp = dataset_timestamp
        self._tags = dict(tags)
        self._metadata = metadata.copy()
        self.columns = columns
        self.constraints = constraints

        self.model_profile = model_profile

        # Store Name attribute
        self._tags["name"] = name

    @property
    def name(self):
        return self._tags["name"]

    @property
    def tags(self):
        return self._tags.copy()

    @property
    def metadata(self):
        return self._metadata.copy()

    @property
    def session_timestamp(self):
        return self._session_timestamp

    @session_timestamp.setter
    def session_timestamp(self, x):
        if x is None:
            x = datetime.datetime.now(datetime.timezone.utc)
        assert isinstance(x, datetime.datetime)
        self._session_timestamp = x

    @property
    def session_timestamp_ms(self):
        """
        Return the session timestamp value in epoch milliseconds.
        """
        return time.to_utc_ms(self.session_timestamp)

    def add_output_field(self, field: Union[str, List[str]]):
        if self.model_profile is None:
            self.model_profile = ModelProfile()
        if isinstance(field, list):
            for field_name in field:
                self.model_profile.add_output_field(field_name)
        else:
            self.model_profile.add_output_field(field)

    def track_metrics(
        self,
        targets: List[Union[str, bool, float, int]],
        predictions: List[Union[str, bool, float, int]],
        scores: List[float] = None,
        model_type: ModelType = None,
        target_field: str = None,
        prediction_field: str = None,
        score_field: str = None,
    ):
        """
        Function to track metrics based on validation data.

        user may also pass the associated attribute names associated with
        target, prediction, and/or score.

        Parameters
        ----------
        targets : List[Union[str, bool, float, int]]
            actual validated values
        predictions : List[Union[str, bool, float, int]]
            inferred/predicted values
        scores : List[float], optional
            assocaited scores for each inferred, all values set to 1 if not
            passed
        target_field : str, optional
            Description
        prediction_field : str, optional
            Description
        score_field : str, optional
            Description
        model_type : ModelType, optional
            Defaul is Classification type.
        target_field : str, optional
        prediction_field : str, optional
        score_field : str, optional
        score_field : str, optional

        """
        if self.model_profile is None:
            self.model_profile = ModelProfile()
        self.model_profile.compute_metrics(
            predictions=predictions,
            targets=targets,
            scores=scores,
            model_type=model_type,
            target_field=target_field,
            prediction_field=prediction_field,
            score_field=score_field,
        )

    def track(self, columns, data=None, character_list=None, token_method=None):
        """
        Add value(s) to tracking statistics for column(s).

        Parameters
        ----------
        columns : str, dict
            Either the name of a column, or a dictionary specifying column
            names and the data (value) for each column
            If a string, `data` must be supplied.  Otherwise, `data` is
            ignored.
        data : object, None
            Value to track.  Specify if `columns` is a string.
        """
        if data is not None:
            if type(columns) != str:
                raise TypeError("Unambiguous column to data mapping")
            self.track_datum(columns, data)
        else:
            if isinstance(columns, dict):
                for column_name, data in columns.items():
                    self.track_datum(column_name, data, character_list=None, token_method=None)
            elif isinstance(columns, str):
                self.track_datum(columns, None, character_list=None, token_method=None)
            else:
                raise TypeError("Data type of: {} not supported for tracking".format(columns.__class__.__name__))

    def track_datum(self, column_name, data, character_list=None, token_method=None):
        try:
            prof = self.columns[column_name]
        except KeyError:
            constraints = None if self.constraints is None else self.constraints[column_name]
            prof = ColumnProfile(column_name, constraints=constraints)
            self.columns[column_name] = prof

        prof.track(data, character_list=None, token_method=None)

    def track_array(self, x: np.ndarray, columns=None):
        """
        Track statistics for a numpy array

        Parameters
        ----------
        x : np.ndarray
            2D array to track.
        columns : list
            Optional column labels
        """
        x = np.asanyarray(x)
        if np.ndim(x) != 2:
            raise ValueError("Expected 2 dimensional array")
        if columns is None:
            columns = np.arange(x.shape[1])
        columns = [str(c) for c in columns]
        return self.track_dataframe(pd.DataFrame(x, columns=columns))

    def track_dataframe(self, df: pd.DataFrame, character_list=None, token_method=None):
        """
        Track statistics for a dataframe

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to track
        """
        # workaround for CUDF due to https://github.com/rapidsai/cudf/issues/6743
        if cudfDataFrame is not None and isinstance(df, cudfDataFrame):
            df = df.to_pandas()
        for col in df.columns:
            col_str = str(col)

            x = df[col].values
            for xi in x:
                self.track(col_str, xi, character_list=None, token_method=None)

    def to_properties(self):
        """
        Return dataset profile related metadata

        Returns
        -------
        properties : DatasetProperties
            The metadata as a protobuf object.
        """
        tags = self.tags
        metadata = self.metadata
        if len(metadata) < 1:
            metadata = None

        session_timestamp = to_utc_ms(self.session_timestamp)
        data_timestamp = to_utc_ms(self.dataset_timestamp)

        return DatasetProperties(
            schema_major_version=SCHEMA_MAJOR_VERSION,
            schema_minor_version=SCHEMA_MINOR_VERSION,
            session_id=self.session_id,
            session_timestamp=session_timestamp,
            data_timestamp=data_timestamp,
            tags=tags,
            metadata=metadata,
        )

    def to_summary(self):
        """
        Generate a summary of the statistics

        Returns
        -------
        summary : DatasetSummary
            Protobuf summary message.
        """
        self.validate()
        column_summaries = {name: colprof.to_summary() for name, colprof in self.columns.items()}

        return DatasetSummary(
            properties=self.to_properties(),
            columns=column_summaries,
        )

    def generate_constraints(self) -> DatasetConstraints:
        """
        Assemble a sparse dict of constraints for all features.

        Returns
        -------
        summary : DatasetConstraints
            Protobuf constraints message.
        """
        self.validate()
        constraints = [(name, col.generate_constraints()) for name, col in self.columns.items()]
        # filter empty constraints
        constraints = [(n, c) for n, c in constraints if c is not None]
        return DatasetConstraints(self.to_properties(), None, dict(constraints))

    def flat_summary(self):
        """
        Generate and flatten a summary of the statistics.

        See :func:`flatten_summary` for a description


        """
        summary = self.to_summary()
        return flatten_summary(summary)

    def _column_message_iterator(self):
        self.validate()
        for k, col in self.columns.items():
            yield col.to_protobuf()

    def chunk_iterator(self):
        """
        Generate an iterator to iterate over chunks of data
        """
        # Generate unique identifier
        marker = self.session_id + str(uuid4())

        # Generate metadata
        properties = self.to_properties()

        yield MessageSegment(
            marker=marker,
            metadata=DatasetMetadataSegment(
                properties=properties,
            ),
        )

        chunked_columns = self._column_message_iterator()
        for msg in columns_chunk_iterator(chunked_columns, marker):
            yield MessageSegment(columns=msg)

    def validate(self):
        """
        Sanity check for this object.  Raises an AssertionError if invalid
        """
        for attr in (
            "name",
            "session_id",
            "session_timestamp",
            "columns",
            "tags",
            "metadata",
        ):
            assert getattr(self, attr) is not None
        assert all(isinstance(tag, str) for tag in self.tags.values())

    def merge(self, other):
        """
        Merge this profile with another dataset profile object.

        We will use metadata and timestamps from the current DatasetProfile in the result.

        This operation will drop the metadata from the 'other' profile object.

        Parameters
        ----------
        other : DatasetProfile

        Returns
        -------
        merged : DatasetProfile
            New, merged DatasetProfile
        """
        self.validate()
        other.validate()

        return self._do_merge(other)

    def _do_merge(self, other):
        columns_set = set(list(self.columns.keys()) + list(other.columns.keys()))

        columns = {}
        for col_name in columns_set:
            constraints = None if self.constraints is None else self.constraints[col_name]
            empty_column = ColumnProfile(col_name, constraints=constraints)
            this_column = self.columns.get(col_name, empty_column)
            other_column = other.columns.get(col_name, empty_column)
            columns[col_name] = this_column.merge(other_column)

        if self.model_profile is not None:
            new_model_profile = self.model_profile.merge(other.model_profile)
        else:
            new_model_profile = other.model_profile

        return DatasetProfile(
            name=self.name,
            session_id=self.session_id,
            session_timestamp=self.session_timestamp,
            dataset_timestamp=self.dataset_timestamp,
            columns=columns,
            tags=self.tags,
            metadata=self.metadata,
            model_profile=new_model_profile,
        )

    def merge_strict(self, other):
        """
        Merge this profile with another dataset profile object. This throws exception
        if session_id, timestamps and tags don't match.

        This operation will drop the metadata from the 'other' profile object.

        Parameters
        ----------
        other : DatasetProfile

        Returns
        -------
        merged : DatasetProfile
            New, merged DatasetProfile
        """
        self.validate()
        other.validate()

        assert self.session_id == other.session_id
        assert self.session_timestamp == other.session_timestamp
        assert self.dataset_timestamp == other.dataset_timestamp
        assert self.tags == other.tags

        return self._do_merge(other)

    def serialize_delimited(self) -> bytes:
        """
        Write out in delimited format (data is prefixed with the length of the
        datastream).

        This is useful when you are streaming multiple dataset profile objects

        Returns
        -------
        data : bytes
            A sequence of bytes
        """
        with io.BytesIO() as f:
            protobuf: DatasetProfileMessage = self.to_protobuf()
            size = protobuf.ByteSize()
            f.write(_VarintBytes(size))
            f.write(protobuf.SerializeToString(deterministic=True))
            return f.getvalue()

    def to_protobuf(self) -> DatasetProfileMessage:
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : DatasetProfileMessage
        """
        properties = self.to_properties()

        if self.model_profile is not None:
            model_profile_msg = self.model_profile.to_protobuf()
        else:
            model_profile_msg = None
        return DatasetProfileMessage(
            properties=properties,
            columns={k: v.to_protobuf() for k, v in self.columns.items()},
            modeProfile=model_profile_msg,
        )

    def write_protobuf(self, protobuf_path: str, delimited_file: bool = True, transport_parameters: dict = None) -> None:
        """
        Write the dataset profile to disk in binary format

        Parameters
        ----------
        protobuf_path : str
            local path or any path supported supported by smart_open:
            https://github.com/RaRe-Technologies/smart_open#how.
            The parent directory must already exist
        delimited_file : bool, optional
            whether to prefix the data with the length of output or not.
            Default is True

        """
        if transport_parameters:
            with open(protobuf_path, "wb", transport_parameters=transport_parameters) as f:
                msg = self.to_protobuf()
                size = msg.ByteSize()
                if delimited_file:
                    f.write(_VarintBytes(size))
                f.write(msg.SerializeToString())
        else:
            with open(protobuf_path, "wb") as f:
                msg = self.to_protobuf()
                size = msg.ByteSize()
                if delimited_file:
                    f.write(_VarintBytes(size))
                f.write(msg.SerializeToString())

    @staticmethod
    def read_protobuf(protobuf_path: str, delimited_file: bool = True, transport_parameters: dict = None) -> "DatasetProfile":
        """
        Parse a protobuf file and return a DatasetProfile object


        Parameters
        ----------
        protobuf_path : str
            the path of the protobuf data, can be local or any other path supported by smart_open: https://github.com/RaRe-Technologies/smart_open#how
        delimited_file : bool, optional
            whether the data is delimited or not. Default is `True`

        Returns
        -------
        DatasetProfile
            whylogs.DatasetProfile object from the protobuf
        """
        with open(protobuf_path, "rb") as f:
            data = f.read()
            if delimited_file:
                msg_len, new_pos = _DecodeVarint32(data, 0)
            else:
                msg_len = len(data)
                new_pos = 0
            msg_buf = data[new_pos : new_pos + msg_len]
            return DatasetProfile.from_protobuf_string(msg_buf)

    @staticmethod
    def from_protobuf(message: DatasetProfileMessage) -> "DatasetProfile":
        """
        Load from a protobuf message

        Parameters
        ----------
        message : DatasetProfileMessage
            The protobuf message.  Should match the output of
            `DatasetProfile.to_protobuf()`

        Returns
        -------
        dataset_profile : DatasetProfile
        """
        properties: DatasetProperties = message.properties
        name = (properties.tags or {}).get("name", None) or (properties.tags or {}).get("Name", None) or ""

        return DatasetProfile(
            name=name,
            session_id=properties.session_id,
            session_timestamp=from_utc_ms(properties.session_timestamp),
            dataset_timestamp=from_utc_ms(properties.data_timestamp),
            columns={k: ColumnProfile.from_protobuf(v) for k, v in message.columns.items()},
            tags=dict(properties.tags or {}),
            metadata=dict(properties.metadata or {}),
            model_profile=ModelProfile.from_protobuf(message.modeProfile),
        )

    @staticmethod
    def from_protobuf_string(data: bytes) -> "DatasetProfile":
        """
        Deserialize a serialized `DatasetProfileMessage`

        Parameters
        ----------
        data : bytes
            The serialized message

        Returns
        -------
        profile : DatasetProfile
            The deserialized dataset profile
        """
        msg = DatasetProfileMessage.FromString(data)
        return DatasetProfile.from_protobuf(msg)

    @staticmethod
    def _parse_delimited_generator(data: bytes):
        pos = 0
        data_len = len(data)
        while pos < data_len:
            pos, profile = DatasetProfile.parse_delimited_single(data, pos)
            yield profile

    @staticmethod
    def parse_delimited_single(data: bytes, pos=0):
        """
        Parse a single delimited entry from a byte stream
        Parameters
        ----------
        data : bytes
            The bytestream
        pos : int
            The starting position. Default is zero

        Returns
        -------
        pos : int
            Current position in the stream after parsing
        profile : DatasetProfile
            A dataset profile
        """
        msg_len, new_pos = _DecodeVarint32(data, pos)
        pos = new_pos
        msg_buf = data[pos : pos + msg_len]
        pos += msg_len
        profile = DatasetProfile.from_protobuf_string(msg_buf)
        return pos, profile

    @staticmethod
    def parse_delimited(data: bytes):
        """
        Parse delimited data (i.e. data prefixed with the message length).

        Java protobuf writes delimited messages, which is convenient for
        storing multiple dataset profiles. This means that the main data is
        prefixed with the length of the message.

        Parameters
        ----------
        data : bytes
            The input byte stream

        Returns
        -------
        profiles : list
            List of all Dataset profile objects

        """
        return list(DatasetProfile._parse_delimited_generator(data))

    def apply_summary_constraints(self, summary_constraints: Optional[Mapping[str, SummaryConstraints]] = None):
        if summary_constraints is None:
            summary_constraints = self.constraints.summary_constraint_map
        for k, v in summary_constraints.items():
            if isinstance(v, list):
                summary_constraints[k] = SummaryConstraints(v)
        for feature_name, constraints in summary_constraints.items():
            if feature_name in self.columns:
                colprof = self.columns[feature_name]
                summ = colprof.to_summary()
                constraints.update(summ.number_summary)
            else:
                logger.debug(f"unkown feature '{feature_name}' in summary constraints")

        return [(k, s.report()) for k, s in summary_constraints.items()]


def columns_chunk_iterator(iterator, marker: str):
    """
    Create an iterator to return column messages in batches

    Parameters
    ----------
    iterator
        An iterator which returns protobuf column messages
    marker
        Value used to mark a group of column messages
    """
    # Initialize
    max_len = COLUMN_CHUNK_MAX_LEN_IN_BYTES
    content_len = 0
    message = ColumnsChunkSegment(marker=marker)

    # Loop over columns
    for col_message in iterator:
        message_len = col_message.ByteSize()
        candidate_content_size = content_len + message_len
        if candidate_content_size <= max_len:
            # Keep appending columns
            message.columns.append(col_message)
            content_len = candidate_content_size
        else:
            yield message
            message = ColumnsChunkSegment(marker=marker)
            message.columns.append(col_message)
            content_len = message_len

    # Take care of any remaining messages
    if len(message.columns) > 0:
        yield message


def dataframe_profile(df: pd.DataFrame, name: str = None, timestamp: datetime.datetime = None):
    """
    Generate a dataset profile for a dataframe

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe to track, treated as a complete dataset.
    name : str
        Name of the dataset
    timestamp : datetime.datetime, float
        Timestamp of the dataset.  Defaults to current UTC time.  Can be a
        datetime or UTC epoch seconds.

    Returns
    -------
    prof : DatasetProfile
    """
    if name is None:
        name = "dataset"
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc)
    elif not isinstance(timestamp, datetime.datetime):
        # Assume UTC epoch seconds
        timestamp = datetime.datetime.utcfromtimestamp(float(timestamp))
    prof = DatasetProfile(name, timestamp)
    prof.track_dataframe(df)
    return prof


def array_profile(
    x: np.ndarray,
    name: str = None,
    timestamp: datetime.datetime = None,
    columns: list = None,
):
    """
    Generate a dataset profile for an array

    Parameters
    ----------
    x : np.ndarray
        Array-like object to track.  Will be treated as an full dataset
    name : str
        Name of the dataset
    timestamp : datetime.datetime
        Timestamp of the dataset.  Defaults to current UTC time
    columns : list
        Optional column labels

    Returns
    -------
    prof : DatasetProfile
    """
    if name is None:
        name = "dataset"
    if timestamp is None:
        timestamp = datetime.datetime.now(datetime.timezone.utc)
    prof = DatasetProfile(name, timestamp)
    prof.track_array(x, columns)
    return prof
