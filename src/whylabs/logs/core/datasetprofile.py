import datetime
import time
from logging import getLogger
from uuid import uuid4

import numpy as np
import pandas as pd
import time
from whylabs.logs.util.protobuf import message_to_json

from whylabs.logs.core import ColumnProfile
from whylabs.logs.core.types.typeddataconverter import TYPES
from whylabs.logs.proto import ColumnsChunkSegment, DatasetProperties
from whylabs.logs.proto import DatasetSummary, DatasetMetadataSegment, \
    MessageSegment, DatasetProfileMessage
from whylabs.logs.util.data import getter, remap, get_valid_filename
from whylabs.logs.util.time import to_utc_ms, from_utc_ms

COLUMN_CHUNK_MAX_LEN_IN_BYTES = int(1e6) - 10
TYPENUM_COLUMN_NAMES = {k: 'type_' + k.lower() + '_count' for k in
                        TYPES.keys()}

SCALAR_NAME_MAPPING = {
    'counters': {
        'count': 'count',
        'null_count': {'value': 'null_count'},
        'true_count': {'value': 'bool_count'},
    },
    'number_summary': {
        'count': 'numeric_count',
        'max': 'max',
        'mean': 'mean',
        'min': 'min',
        'stddev': 'stddev',
        'unique_count': {
            'estimate': 'nunique_numbers',
            'lower': 'nunique_numbers_lower',
            'upper': 'nunique_numbers_upper'
        }
    },
    'schema': {
        'inferred_type': {
            'type': 'inferred_dtype',
            'ratio': 'dtype_fraction',
        },
        'type_counts': TYPENUM_COLUMN_NAMES,
    },
    'string_summary': {
        'unique_count': {
            'estimate': 'nunique_str',
            'lower': 'nunique_str_lower',
            'upper': 'ununique_str_upper'
        }
    }
}


class DatasetProfile:
    """
    Statistics tracking for a dataset.

    A dataset refers to a collection of columns.

    Parameters
    ----------
    name: str
        A human readable name for the dataset profile. Could be model name
    data_timestamp: datetime.datetime
        The timestamp associated with the data (i.e. batch run). Optional.
    session_timestamp : datetime.datetime
        Timestamp of the dataset
    columns : dict
        Dictionary lookup of `ColumnProfile`s
    tags : list-like
        A list (or tuple, or iterable) of dataset tags.
    metadata: dict
        Metadata. Could be arbitratry strings.
    session_id : str
        The unique session ID run. Should be a UUID.
    """

    def __init__(self,
                 name: str,
                 data_timestamp: datetime.datetime = None,
                 session_timestamp: datetime.datetime = None,
                 columns: dict = None,
                 tags=None,
                 metadata=None,
                 session_id: str = None):
        # Default values
        if columns is None:
            columns = {}
        if tags is None:
            tags = []
        if metadata is None:
            metadata = dict()
        if session_id is None:
            session_id = uuid4().hex
        if session_timestamp is None:
            session_timestamp = datetime.datetime.now(datetime.timezone.utc)
        if name is not None:
            metadata['Name'] = name

        # Store attributes
        self.name = name
        self.session_id = session_id
        self.session_timestamp = session_timestamp
        self.data_timestamp = data_timestamp
        self.tags = tuple(sorted([tag for tag in tags]))
        self.metadata = metadata
        self.columns = columns

    @property
    def timestamp_ms(self):
        """
        Return the timestamp value in epoch milliseconds
        """
        # TODO: Implement proper timestamp conversion
        return 1588978362910  # Some made up timestamp in milliseconds

    def track(self, columns, data=None):
        """
        Add value(s) to tracking statistics for column(s)

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
            self._track_single_column(columns, data)
        else:
            for column_name, data in columns.items():
                self._track_single_column(column_name, data)

    def _track_single_column(self, column_name, data):
        try:
            prof = self.columns[column_name]
        except KeyError:
            prof = ColumnProfile(column_name)
            self.columns[column_name] = prof
        prof.track(data)

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

    def track_dataframe(self, df: pd.DataFrame):
        """
        Track statistics for a dataframe

        Parameters
        ----------
        df : pandas.DataFrame
            DataFrame to track
        """
        for col in df.columns:
            col_str = str(col)
            x = df[col].values
            for xi in x:
                self.track(col_str, xi)

    def to_properties(self):
        tags = self.tags
        if len(tags) < 1:
            tags = None
        metadata = self.metadata
        if len(metadata) < 1:
            metadata = None

        session_timestamp = to_utc_ms(self.session_timestamp)
        data_timestamp = to_utc_ms(self.data_timestamp)

        return DatasetProperties(
            schema_major_version=1,
            schema_minor_version=0,
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
        column_summaries = {name: colprof.to_summary()
                            for name, colprof in self.columns.items()}

        return DatasetSummary(
            properties=self.to_properties(),
            columns=column_summaries,
        )

    def flat_summary(self):
        """
        Generate and flatten a summary of the statistics

        Returns
        -------
        summary : pd.DataFrame
            Per-column summary statistics
        hist : dict
            Dictionary of histograms with (column name, histogram) key, value
            pairs.  Histograms are formatted as a `pandas.Series`
        frequent_strings : dict
            Dictionary of frequent string counts with (column name, counts)
            key, val pairs.  `counts` are a pandas Series.
        """
        summary = self.to_summary()
        return flatten_summary(summary)

    def _column_message_iterator(self):
        self.validate()
        for col in self.columns.items():
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
            )
        )

        chunked_columns = self._column_message_iterator()
        for msg in columns_chunk_iterator(chunked_columns, marker):
            yield MessageSegment(columns=msg)

    def validate(self):
        """Sanity check for this object.  Raises an Exception if invalid"""
        for attr in ('name', 'session_id', 'session_timestamp', 'columns', 'tags', 'metadata'):
            assert getattr(self, attr) is not None
        tags = self.tags
        assert all(isinstance(tag, str) for tag in self.tags)
        if not all(tags[i] <= tags[i + 1] for i in range(len(tags) - 1)):
            raise ValueError("Tags must be sorted")

    def merge(self, other):
        """
        Merge this profile with another.

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

        assert self.name == other.name
        assert self.session_id == other.session_id
        assert self.session_timestamp == other.session_timestamp
        assert self.data_timestamp == other.data_timestamp
        assert self.metadata == other.metadata
        assert self.tags == other.tags

        columns_set = set(list(self.columns.keys()) + list(other.columns.keys()))
        columns = {}
        for col_name in columns_set:
            empty_column = ColumnProfile(col_name)
            this_column = self.columns.get(col_name, empty_column)
            other_column = other.columns.get(col_name, empty_column)
            columns[col_name] = this_column.merge(other_column)

        return DatasetProfile(
            name=self.name,
            session_id=self.session_id,
            session_timestamp=self.session_timestamp,
            data_timestamp=self.data_timestamp,
            columns=columns,
            tags=self.tags,
            metadata=self.metadata
        )

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : DatasetProfileMessage
        """
        properties = self.to_properties()

        return DatasetProfileMessage(
            properties=properties,
            columns={k: v.to_protobuf() for k, v in self.columns.items()},
        )

    def write(self, file_prefix=None, profile=True, flat_summary=True,
              json_summary=False, dataframe_fmt='csv'):
        """
        Utility function to simplify writing of this dataset profile.

        Parameters
        ----------
        file_prefix
        profile
        flat_summary
        json_summary
        dataframe_fmt

        Returns
        -------

        """
        logger = getLogger(__name__)
        output_files = {}
        if file_prefix is None:
            file_prefix = f'{self.name}_{int(time.time())}'
        if flat_summary or json_summary:
            summary = self.to_summary()
        if flat_summary:
            x = flatten_summary(summary)
            fnames = write_flat_dataset_summary(
                x, file_prefix, dataframe_fmt=dataframe_fmt)
            output_files['flat_summary'] = fnames
        if json_summary:
            fname = file_prefix + '.json'
            with open(fname, 'wt') as fp:
                logger.debug(f"Writing JSON summaries to: {fname}")
                fp.write(message_to_json(summary))
            output_files['json'] = json_summary

        if profile:
            fname = file_prefix + '.bin'
            msg = self.to_protobuf()
            with open(fname, 'wb') as fp:
                logger.debug(f'Writing protobuf profile to: {fname}')
                fp.write(msg.SerializeToString())
            output_files['protobuf'] = fname

    @staticmethod
    def from_protobuf(message):
        """
        Load from a protobuf message

        Returns
        -------
        dataset_profile : DatasetProfile
        """
        return DatasetProfile(
            name=message.properties.metadata['Name'],
            session_id=message.properties.session_id,
            session_timestamp=from_utc_ms(message.properties.session_timestamp),
            data_timestamp=from_utc_ms(message.properties.data_timestamp),
            columns={k: ColumnProfile.from_protobuf(v)
                     for k, v in message.columns.items()},
            tags=message.properties.tags,
            metadata=message.properties.metadata
        )


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


def flatten_summary(dataset_summary: DatasetSummary):
    """
    Flatten a DatasetSummary

    Parameters
    ----------
    dataset_summary : DatasetSummary
        Summary to flatten

    Returns
    -------
    A dictionary with the following keys:

    summary : pd.DataFrame
        Per-column summary statistics
    hist : pd.Series
        Series of histogram Series with (column name, histogram) key, value
        pairs.  Histograms are formatted as a `pandas.Series`
    frequent_strings : pd.Series
        Series of frequent string counts with (column name, counts)
        key, val pairs.  `counts` are a pandas Series.
    """
    hist = flatten_dataset_histograms(dataset_summary)
    frequent_strings = flatten_dataset_frequent_strings(dataset_summary)
    summary = get_dataset_frame(dataset_summary)
    return {
        'summary': summary,
        'hist': hist,
        'frequent_strings': frequent_strings
    }


def flatten_dataset_histograms(dataset_summary):
    """
    Flatten histograms from a dataset summary
    """
    histograms = {}

    for col_name, col in dataset_summary.columns.items():
        try:
            hist = getter(getter(col, 'number_summary'), 'histogram')
            if len(hist.bins) > 1:
                histograms[col_name] = {
                    'bin_edges': list(hist.bins),
                    'counts': list(hist.counts),
                }
        except KeyError:
            continue
    return histograms


def flatten_dataset_frequent_strings(dataset_summary):
    """
    Flatten frequent strings summaries from a dataset summary
    """
    frequent_strings = {}

    for col_name, col in dataset_summary.columns.items():
        try:
            item_summary = getter(getter(col, 'string_summary'), 'frequent') \
                .items
            items = {}
            for item in item_summary:
                items[item.value] = int(item.estimate)
            if len(items) > 0:
                frequent_strings[col_name] = items
        except KeyError:
            continue

    return frequent_strings


def get_dataset_frame(dataset_summary, mapping: dict = None):
    """
    Get a dataframe from scalar values flattened from a dataset summary

    Returns
    -------
    summary : pd.DataFrame
        Scalar values, flattened and re-named according to `mapping`
    """
    import pandas as pd
    if mapping is None:
        mapping = SCALAR_NAME_MAPPING
    col_out = {}
    for k, col in dataset_summary.columns.items():
        col_out[k] = remap(col, mapping)
    scalar_summary = pd.DataFrame(col_out).T
    scalar_summary.index.session_id = 'column'
    return scalar_summary.reset_index()


def write_flat_dataset_summary(summary, prefix: str, dataframe_fmt: str = 'csv'):
    """
    Utility to write a flattened dataset summary to disk.

    Parameters
    ----------
    summary : dict, DatasetSummary
        The dataset summary.  Either a `DatasetSummary` protobuf message or the
        the dictionary output of `flatten_summary()`
    prefix : str
        Output path prefix, used to construct output file names.
    dataframe_fmt : str
        File format for the summary table.  Other files will be output as
        json.  Formats: 'csv', 'parquet'

    Returns
    -------
    filenames : dict
        Dictionary containing output paths
    """
    import json
    if not isinstance(summary, dict):
        summary = flatten_summary(summary)
    logger = getLogger(__name__)

    # Extract the already flattened summary data
    df = summary['summary']
    hist = summary['hist']
    strings = summary['frequent_strings']

    # Save summary table
    df_name = f'{prefix}_summary.{dataframe_fmt}'
    if dataframe_fmt == 'csv':
        df.to_csv(df_name, index=False)
    elif dataframe_fmt == 'parquet':
        df.to_parquet(df_name, engine='pyarrow', compression='snappy')
    else:
        raise ValueError(f"Unrecognized format: {dataframe_fmt}")

    # Save per-column histograms
    hist_name = f'{prefix}_histogram.json'
    json.dump(hist, open(hist_name, 'wt'), indent=4)
    logger.debug(f'Saved histograms to: {hist_name}')

    # Save per-column string counts
    strings_name = f'{prefix}_strings.json'
    json.dump(strings, open(strings_name, 'wt'), indent=4)
    logger.debug(f'Saved frequent string counts to: {strings_name}')

    return {
        'dataframe': df_name,
        'histogram': hist_name,
        'strings': strings_name
    }


def write_flat_summaries(summaries, prefix: str,
                         dataframe_fmt: str = 'csv'):
    """
    Utility to write flattened `DatasetSummaries` to disk.

    Parameters
    ----------
    summaries : DatasetSummaries
        DatasetSummaries protobuf message
    prefix : str
        Output path prefix, used to construct output file names.
    dataframe_fmt : str
        File format for the summary tables.  Other files will be output as
        json.  Formats: 'csv', 'parquet'

    Returns
    -------
    filenames : dict
        Dictionary containing output filenames
    """
    from collections import defaultdict
    fnames = defaultdict(list)
    for name, summary in summaries.profiles.items():
        fullprefix = prefix + '_' + get_valid_filename(name)
        x = write_flat_dataset_summary(summary, fullprefix, dataframe_fmt)
        for k, v in x.items():
            fnames[k].append(v)
    return dict(fnames)


def dataframe_profile(df: pd.DataFrame, name: str = None,
                      timestamp: datetime.datetime = None):
    """
    Generate a dataset profile for a dataframe

    Parameters
    ----------
    df : pandas.DataFrame
        Dataframe to track, treated as a complete dataset.
    name : str
        Name of the dataset (e.g. timestamp string)
    timestamp : datetime.datetime, float
        Timestamp of the dataset.  Defaults to current UTC time.  Can be a
        datetime or UTC epoch seconds.
    """
    if name is None:
        name = 'dataset'
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()
    elif not isinstance(timestamp, datetime.datetime):
        # Assume UTC epoch seconds
        timestamp = datetime.datetime.utcfromtimestamp(float(timestamp))
    prof = DatasetProfile(name, timestamp)
    prof.track_dataframe(df)
    return prof


def array_profile(x: np.ndarray, name: str = None,
                  timestamp: datetime.datetime = None, columns: list = None):
    """
    Generate a dataset profile for an array

    Parameters
    ----------
    x : np.ndarray
        Array-like object to track.  Will be treated as an full dataset
    name : str
        Name of the dataset (e.g. timestamp string)
    timestamp : datetime.datetime
        Timestamp of the dataset.  Defaults to current UTC time
    columns : list
        Optional column labels
    """
    if name is None:
        name = 'dataset'
    if timestamp is None:
        timestamp = datetime.datetime.utcnow()
    prof = DatasetProfile(name, timestamp)
    prof.track_array(x, columns)
    return prof
