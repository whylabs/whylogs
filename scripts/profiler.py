#!/usr/bin/env python3
"""
TODO:
    * Date parsing compatible with EasyDateTimeParser (Java)

"""
from logging import getLogger

from whylogs.util import protobuf

CSV_READER_BATCH_SIZE = int(1e4)
OUTPUT_DATE_FORMAT = "%Y/%m/%d"
LOGGER = "whylogs.logs.profiler"


def write_protobuf(vals: list, fname):
    """
    Write a list of objects with a `to_protobuf()` method to a binary file.

    `vals` must be iterable
    """
    serialized = [x.to_protobuf() for x in vals]
    getLogger(LOGGER).info("Writing to protobuf binary file: {}".format(fname))
    protobuf.write_multi_msg(serialized, fname)


def df_to_records(df, dropna=True):
    """
    Convert a dataframe to a list of dictionaries, one per row, dropping null
    values
    """
    import pandas as pd

    records = df.to_dict(orient="records")
    if dropna:
        records = [{k: v for k, v in m.items() if pd.notnull(v)} for m in records]
    return records


def csv_reader(f, date_format: str = None, dropna=False, infer_dtypes=False, **kwargs):
    """
    Wrapper for `pandas.read_csv` to return an iterator to return dict
    records for a CSV file

    See also `pandas.read_csv`

    Parameters
    ----------
    f : str, path object, or file-like object
        File to read from.  See `pandas.read_csv` documentation
    date_format : str
        If specified, string format for the date.  See `pd.datetime.strptime`
    dropna : bool
        Remove null values from returned records
    infer_dtypes : bool
        Automatically infer data types (standard pandas behavior).  Else,
        return all items as strings (except specified date columns)
    **kwargs : passed to `pandas.read_csv`
    """
    import pandas as pd

    date_parser = None
    if date_format is not None:

        def date_parser(x):
            return pd.datetime.strptime(x, date_format)  # noqa pep8

    opts = {
        "chunksize": CSV_READER_BATCH_SIZE,
        "date_parser": date_parser,
    }
    if not infer_dtypes:
        # HACKY way not parse any entries and return strings
        opts["converters"] = {i: str for i in range(10000)}
    opts.update(kwargs)

    for batch in pd.read_csv(f, **opts):
        records = df_to_records(batch, dropna=dropna)
        for record in records:
            yield record


def run(
    input_path,
    datetime: str = None,
    delivery_stream=None,
    fmt=None,
    limit=-1,
    output_prefix=None,
    region=None,
    separator=None,
    dropna=False,
    infer_dtypes=False,
):
    """
    Run the profiler on CSV data

    Output Notes
    ------------
    <output_prefix>_<name>_summary.csv
        Dataset profile.  Contains scalar statistics per column
    <output_prefix>_<name>_histogram.json
        Histograms for each column for dataset `name`
    <output_prefix>_<name>_strings.json
        Frequent strings
    <output_prefix>.json
        DatasetSummaries, nested JSON summaries of dataset statistics
    <output_prefix>.bin
        Binary protobuf output of DatasetProfile


    Parameters
    ----------
    input_path : str
        Input CSV file
    datetime : str
        Column containing timestamps.  If missing, we assume the dataset is
        running in batch mode
    delivery_stream : str
        [IGNORED] The delivery stream name
    fmt : str
        Format of the datetime column, used if `datetime` is specified.
        If not specified, the format will be attempt to be inferred.
    limit : int
        Limit the number of entries to processes
    output_prefix : str
        Specify a prefix for the output files.  By default, this will be
        derived from the input path to generate files in the input directory.
        Can include folders
    region : str
        [IGNORED] AWS region name for Firehose
    separator : str
        Record separator.  Default = ','
    dropna : bool
        Drop null values when reading
    infer_dtypes : bool
        Infer input datatypes when reading.  If false, treat inputs as
        un-converted strings.
    """
    datetime_col = datetime  # don't shadow the standard module name
    import os
    from datetime import datetime

    from whylogs.logs import DatasetProfile, DatasetSummaries
    from whylogs.util import message_to_json

    logger = getLogger(LOGGER)

    # Parse arguments
    if separator is None:
        separator = ","
    name = os.path.basename(input_path)
    parse_dates = False
    if datetime_col is not None:
        parse_dates = [datetime_col]
    nrows = None
    if limit > 0:
        nrows = limit
    if output_prefix is None:
        import random
        import time

        parent_folder = os.path.dirname(os.path.realpath(input_path))
        basename = os.path.splitext(os.path.basename(input_path))[0]
        epoch_minutes = int(time.time() / 60)
        output_base = "{}.{}-{}-{}".format(
            basename,
            epoch_minutes,
            random.randint(100000, 999999),
            random.randint(100000, 999999),
        )
        output_prefix = os.path.join(parent_folder, output_base)

    output_base = output_prefix
    binary_output_path = output_base + ".bin"
    json_output_path = output_base + ".json"

    # Process records
    reader = csv_reader(
        input_path,
        fmt,
        parse_dates=parse_dates,
        nrows=nrows,
        sep=separator,
        dropna=dropna,
        infer_dtypes=infer_dtypes,
    )
    profiles = {}
    for record in reader:
        dt = record.get(datetime_col, datetime.now(datetime.timezone.utc))
        assert isinstance(dt, datetime)
        dt_str = dt.strftime(OUTPUT_DATE_FORMAT)
        try:
            ds = profiles[dt_str]
        except KeyError:
            ds = DatasetProfile(name, dt)
            profiles[dt_str] = ds
        ds.track(record)

    logger.info("Finished collecting statistics")

    # Build summaries for the JSON output
    summaries = DatasetSummaries(profiles={k: v.to_summary() for k, v in profiles.items()})
    with open(json_output_path, "wt") as fp:
        logger.info("Writing JSON summaries to: {}".format(json_output_path))
        fp.write(message_to_json(summaries))

    # Write the protobuf binary file
    write_protobuf(profiles.values(), binary_output_path)
    return profiles


if __name__ == "__main__":
    import argh

    from whylogs.logs import display_logging

    display_logging("DEBUG")
    argh.dispatch_command(run)
