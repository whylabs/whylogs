import math
from typing import Dict, List, Optional, Union, Tuple

import pyspark
from pyspark.sql import functions as F
from pyspark.sql import SparkSession, Window


def _simple_entropy(df: pyspark.sql.dataframe.DataFrame,
                    column_name: str) -> float:
    count = df.count()
    testdf = df.select(column_name).groupby(column_name).agg((F.count(column_name) / count).alias("p"))
    result = testdf.groupby().agg(-F.sum(F.col("p") * F.log2("p"))).collect()[0][0]
    if not result:
        return 0.0
    return result


def _weighted_entropy(countdf: pyspark.sql.dataframe.DataFrame,
                      total_count: int,
                      split_columns: Optional[List[str]],
                      target_column_name: str,
                      weighted: bool = True) -> float:
    """Entropy calculation across many ."""
    split_columns_plus_target = split_columns[:]
    split_columns_plus_target.append(target_column_name)
    groupdf = countdf.groupby(split_columns_plus_target) \
                     .agg(F.sum("count").alias("group_count"))

    w = Window.partitionBy(split_columns)
    groupdf = groupdf.withColumn("p",
                                 F.col('group_count') / F.sum(groupdf['group_count']).over(w)) \
                     .withColumn("weight",
                                 F.sum(groupdf['group_count'] / total_count).over(w))

    entropydf = groupdf.groupby(split_columns).agg(
            (-F.sum(F.col("p") * F.log2("p"))).alias("entropy"),
            (F.sum(F.col("group_count") / total_count)).alias("weight"))

    if weighted:
        result = entropydf.groupby().agg(F.sum(F.col("entropy") * F.col("weight"))).collect()[0][0]
    else:
        result = entropydf.groupby().sum("entropy").collect()[0][0]

    return result


def _find_best_split(countdf: pyspark.sql.dataframe.DataFrame,
                     prev_split_columns: List[str],
                     valid_column_names: List[str],
                     target_column_name: str,
                     normalization: Optional[Dict[str, int]] = None) -> Tuple[float, str]:
    total_count = countdf.count()

    max_score_tuple = 0.0, None
    pre_split_entropy = _weighted_entropy(countdf, total_count, prev_split_columns,
                                          target_column_name, True)

    for column_name in valid_column_names:
        if column_name == target_column_name: continue
        new_split_columns = prev_split_columns[:]
        new_split_columns.append(column_name)
        post_split_entropy = _weighted_entropy(countdf, total_count,
                                               new_split_columns,
                                               target_column_name, True)
        value = pre_split_entropy - post_split_entropy

        if normalization and normalization[column_name] > 0:
            value /= math.log(normalization[column_name])

        if value > max_score_tuple[0]:
            max_score_tuple = value, column_name

    return max_score_tuple


def estimate_segments(df: pyspark.sql.dataframe.DataFrame,
                      target_field: str = None,
                      max_segments: int = 30,
                      include_columns: List[str] = [],
                      unique_perc_bounds: Tuple[float, float] = [None, 0.8],
                      null_perc_bounds: Tuple[float, float] = [None, 0.2]) -> Optional[Union[List[Dict], List[str]]]:
    """
    Estimates the most important features and values on which to segment
    data profiling using entropy-based methods.

    If no target column provided, maximum entropy column is substituted.

    :param df: the dataframe of data to profile
    :param target_field: target field (optional)
    :param max_segments: upper threshold for total combinations of segments,
    default 30
    :param include_columns: additional non-string columns to consider in automatic segmentation. Warning: high cardinality columns will degrade performance.
    :param unique_perc_bounds: tuple of form [lower, upper] with bounds on the percentage of unique values (|unique| / |X|). Upper bound exclusive.
    :param null_perc_bounds: tuple of form [lower, upper] with bounds on the percentage of null values. Upper bound exclusive.
    :return: a list of segmentation feature names
    """
    current_split_columns = []
    segments = []
    segments_used = 1
    max_entropy_column = (float('-inf'), None)

    if not unique_perc_bounds[0]: unique_perc_bounds[0] = float("-inf")
    if not unique_perc_bounds[1]: unique_perc_bounds[1] = float("inf")
    if not null_perc_bounds[0]: null_perc_bounds[0] = float("-inf")
    if not null_perc_bounds[1]: null_perc_bounds[1] = float("inf")

    valid_column_names = set()

    count = df.count()

    print("Limiting to categorical (string) data columns...")
    valid_column_names = {col for col in df.columns if \
            (df.select(col).dtypes[0][1] == 'string' or \
             col in include_columns)}

    print("Gathering cardinality information...")
    n_uniques = {col: df.agg(
            F.approx_count_distinct(col)).collect()[0][0] for col in valid_column_names}
    print("Gathering missing value information...")
    n_nulls = {col: df.filter(df[col].isNull()).count() for col in valid_column_names}

    print("Finding valid columns for autosegmentation...")
    for col in valid_column_names.copy():
        null_perc = 0.0 if count == 0 else n_nulls[col] / count
        unique_perc = 0.0 if count == 0 else n_uniques[col] / count
        if (col in segments or
            n_uniques[col] <= 1 or
            null_perc < null_perc_bounds[0] or
            null_perc >= null_perc_bounds[1] or
            unique_perc < unique_perc_bounds[0] or
            unique_perc >= unique_perc_bounds[1]):
            valid_column_names.remove(col)

    if not valid_column_names:
        return []

    if not target_field:
        print("Finding alternative target field since none were specified...")
        for col in valid_column_names:
            col_entropy = _simple_entropy(df, col)
            if n_uniques[col] > 1:
                col_entropy /= math.log(n_uniques[col])
            if col_entropy > max_entropy_column[0]:
                max_entropy_column = (col_entropy, col)
        target_field = max_entropy_column[1]

    print(f"Using {target_field} column as target field.")
    assert target_field in df.columns
    valid_column_names.add(target_field)
    valid_column_names = list(valid_column_names)

    countdf = df.select(valid_column_names).groupby(valid_column_names).count().cache()

    print(f"Calculating segments...")
    while segments_used < max_segments:
        valid_column_names = {col for col in valid_column_names \
                              if (col not in segments and \
                              n_uniques[col] * segments_used <= (max_segments - segments_used))}
        _, segment_column_name = _find_best_split(countdf,
                                                  current_split_columns,
                                                  list(valid_column_names),
                                                  target_column_name=target_field,
                                                  normalization=n_uniques)

        if not segment_column_name:
            break

        segments.append(segment_column_name)
        current_split_columns.append(segment_column_name)
        segments_used *= n_uniques[segment_column_name]

    return segments
