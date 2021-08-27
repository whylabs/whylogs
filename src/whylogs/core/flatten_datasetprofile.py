from collections import OrderedDict

import pandas as pd

from whylogs.core.types.typeddataconverter import TYPES
from whylogs.proto import DatasetSummary
from whylogs.util.data import getter, remap

TYPENUM_COLUMN_NAMES = OrderedDict()
for k in TYPES.keys():
    TYPENUM_COLUMN_NAMES[k] = "type_" + k.lower() + "_count"
# NOTE: I use ordered dicts here to control the ordering of generated columns
# dictionaries are also valid
#: Define (some of) the mapping from dataset summary to flat table
SCALAR_NAME_MAPPING = OrderedDict(
    counters=OrderedDict(
        count="count",
        null_count=OrderedDict(value="null_count"),
        true_count=OrderedDict(value="bool_count"),
    ),
    number_summary=OrderedDict(
        count="numeric_count",
        max="max",
        mean="mean",
        min="min",
        stddev="stddev",
        unique_count=OrderedDict(
            estimate="nunique_numbers",
            lower="nunique_numbers_lower",
            upper="nunique_numbers_upper",
        ),
    ),
    schema=OrderedDict(
        inferred_type=OrderedDict(type="inferred_dtype", ratio="dtype_fraction"),
        type_counts=TYPENUM_COLUMN_NAMES,
    ),
    string_summary=OrderedDict(
        unique_count=OrderedDict(
            estimate="nunique_str",
            lower="nunique_str_lower",
            upper="nunique_str_upper",
        ),
        length=OrderedDict(
            max="max_length",
            mean="mean_length",
            min="min_length",
            stddev="stddev_length",
        ),
        token_length=OrderedDict(
            max="max_token_length",
            mean="mean_token_length",
            min="min_token_length",
            stddev="stddev_token_length",
        ),
    ),
)


def flatten_summary(dataset_summary: DatasetSummary) -> dict:
    """
    Flatten a DatasetSummary

    Parameters
    ----------
    dataset_summary : DatasetSummary
        Summary to flatten

    Returns
    -------
    data : dict
        A dictionary with the following keys:

            summary : pandas.DataFrame
                Per-column summary statistics
            hist : pandas.Series
                Series of histogram Series with (column name, histogram) key,
                value pairs.  Histograms are formatted as a `pandas.Series`
            frequent_strings : pandas.Series
                Series of frequent string counts with (column name, counts)
                key, val pairs.  `counts` are a pandas Series.

    Notes
    -----
    Some relevant info on the summary mapping:

    .. code-block:: python

        >>> from whylogs.core.datasetprofile import SCALAR_NAME_MAPPING
        >>> import json
        >>> print(json.dumps(SCALAR_NAME_MAPPING, indent=2))
    """
    hist = flatten_dataset_histograms(dataset_summary)
    frequent_strings = flatten_dataset_frequent_strings(dataset_summary)
    summary = get_dataset_frame(dataset_summary)
    return {
        "summary": summary,
        "hist": hist,
        "frequent_strings": frequent_strings,
    }


def _quantile_strings(quantiles: list):
    return ["quantile_{:.4f}".format(q) for q in quantiles]


def flatten_dataset_quantiles(dataset_summary: DatasetSummary):
    """
    Flatten quantiles from a dataset summary
    """
    quants = {}
    for col_name, col in dataset_summary.columns.items():
        try:
            quant = getter(getter(col, "number_summary"), "quantiles")
            x = OrderedDict()
            for q, qval in zip(_quantile_strings(quant.quantiles), quant.quantile_values):
                x[q] = qval
            quants[col_name] = x
        except KeyError:
            pass

    return quants


def flatten_dataset_string_quantiles(dataset_summary: DatasetSummary):
    """
    Flatten quantiles from a dataset summary
    """
    quants = {}
    for col_name, col in dataset_summary.columns.items():
        try:
            quant = getter(getter(col, "number_summary"), "quantiles")
            x = OrderedDict()
            for q, qval in zip(_quantile_strings(quant.quantiles), quant.quantile_values):
                x[q] = qval
            quants[col_name] = x
        except KeyError:
            pass

    return quants


def flatten_dataset_histograms(dataset_summary: DatasetSummary):
    """
    Flatten histograms from a dataset summary
    """
    histograms = {}

    for col_name, col in dataset_summary.columns.items():
        try:
            hist = getter(getter(col, "number_summary"), "histogram")
            if len(hist.bins) > 1:
                histograms[col_name] = {
                    "bin_edges": list(hist.bins),
                    "counts": list(hist.counts),
                }
        except KeyError:
            continue
    return histograms


def flatten_dataset_frequent_strings(dataset_summary: DatasetSummary):
    """
    Flatten frequent strings summaries from a dataset summary
    """
    frequent_strings = {}

    for col_name, col in dataset_summary.columns.items():
        try:
            item_summary = getter(getter(col, "string_summary"), "frequent").items
            items = {item.value: int(item.estimate) for item in item_summary}
            if items:
                frequent_strings[col_name] = items
        except KeyError:
            continue

    return frequent_strings


def get_dataset_frame(dataset_summary: DatasetSummary, mapping: dict = None):
    """
    Get a dataframe from scalar values flattened from a dataset summary

    Parameters
    ----------
    dataset_summary : DatasetSummary
        The dataset summary.
    mapping : dict, optional
        Override the default variable mapping.

    Returns
    -------
    summary : pd.DataFrame
        Scalar values, flattened and re-named according to `mapping`
    """
    if mapping is None:
        mapping = SCALAR_NAME_MAPPING
    quantile = flatten_dataset_quantiles(dataset_summary)
    col_out = {}
    for _k, col in dataset_summary.columns.items():
        col_out[_k] = remap(col, mapping)
        col_out[_k].update(quantile.get(_k, {}))
    scalar_summary = pd.DataFrame(col_out).T
    scalar_summary.index.name = "column"
    return scalar_summary.reset_index()
