from typing import Dict, List, Optional, Union

import numpy as np
import pandas as pd


def _entropy(series: pd.Series, normalized: bool = True) -> np.float64:
    """Entropy calculation. If normalized, use log cardinality."""
    probs = series.value_counts(normalize=True, dropna=False)
    entropy = -np.sum([p_i * np.log(p_i) for p_i in probs])
    if normalized:
        if len(series.unique()) > 1:
            entropy /= np.log(len(series.unique()))

    return entropy


def _weighted_entropy(df: pd.DataFrame, split_columns: List[Optional[str]], target_column_name: str, normalized: bool = True):
    """Entropy calculation. If normalized, use log cardinality."""
    weight_split_entropy = 0

    if split_columns:
        col_groups = df.groupby(split_columns)
    else:
        col_groups = [(None, df)]

    for x, col_group in col_groups:
        weight_split_entropy += _entropy(col_group.loc[:, target_column_name], normalized) * len(col_group) / len(df)

    return weight_split_entropy


def _information_gain_ratio(df: pd.DataFrame, prev_split_columns: List[Optional[str]], column_name: str, target_column_name: str, normalized: bool = True):
    """Entropy calculation. If normalized, use log cardinality."""
    pre_split_entropy = _weighted_entropy(df, prev_split_columns, target_column_name, normalized)
    new_split_columns = prev_split_columns[:]
    new_split_columns.append(column_name)
    post_split_entropy = _weighted_entropy(df, new_split_columns, target_column_name, normalized)

    return pre_split_entropy - post_split_entropy


def _find_best_split(df: pd.DataFrame, prev_split_columns: List[str], valid_column_names: List[str], target_column_name: str):

    max_score_tuple = -np.inf, None
    for column_name in valid_column_names:
        value = _information_gain_ratio(df, prev_split_columns, column_name, target_column_name, normalized=True)
        if value > max_score_tuple[0]:
            max_score_tuple = value, column_name

    return max_score_tuple


def _estimate_segments(df: pd.DataFrame, target_field: str = None, max_segments: int = 30) -> Optional[Union[List[Dict], List[str]]]:
    """
    Estimates the most important features and values on which to segment
    data profiling using entropy-based methods.

    If no target column provided, maximum entropy column is substituted.

    :param df: the dataframe of data to profile
    :param target_field: target field (optional)
    :param max_segments: upper threshold for total combinations of segments,
    default 30
    :return: a list of segmentation feature names
    """
    current_split_columns = []
    segments = []
    segments_used = 1
    max_entropy_column = (-np.inf, None)

    if not target_field:
        for col in df.columns:
            col_entropy = _entropy(df.loc[:, col], normalized=True)
            if col_entropy > max_entropy_column[0]:
                max_entropy_column = (col_entropy, col)
        target_field = max_entropy_column[1]

    while segments_used < max_segments:
        valid_column_names = []

        for col in df.columns:

            n_unique = len(df.loc[:, col].unique())
            nulls = df[col].isnull().value_counts(normalize=True)
            null_perc = 0.0 if True not in nulls.index else nulls[True]
            unique_perc = n_unique / len(df[col]) if len(df[col]) > 0 else 0.0
            if n_unique > 1 and n_unique * segments_used <= max_segments - segments_used and col not in segments and null_perc <= 0.2 and unique_perc <= 0.8:
                valid_column_names.append(col)

        if not valid_column_names:
            break

        if target_field in valid_column_names:
            valid_column_names.remove(target_field)

        _, segment_column_name = _find_best_split(df, current_split_columns, valid_column_names, target_column_name=target_field)

        if not segment_column_name:
            break

        segments.append(segment_column_name)
        current_split_columns.append(segment_column_name)
        segments_used *= len(df[segment_column_name].unique())

    return segments
