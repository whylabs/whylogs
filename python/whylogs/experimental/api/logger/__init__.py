import logging
import math
from typing import List, Optional, Set, Tuple, Union

from whylogs.api.logger import log
from whylogs.api.logger.result_set import SegmentedResultSet, ViewResultSet
from whylogs.core import DatasetSchema
from whylogs.core.stubs import np, pd

diagnostic_logger = logging.getLogger(__name__)


def _convert_to_int_if_bool(data: pd.core.frame.DataFrame, *columns: str) -> pd.core.frame.DataFrame:
    for col in columns:
        if all(isinstance(x, bool) for x in data[col]):
            data[col] = data[col].apply(lambda x: 1 if x else 0)
    return data


class RowWiseMetrics:
    def __init__(
        self,
        target_column: str,
        prediction_column: str,
        convert_non_numeric: bool = False,
    ):
        self.target_column = target_column
        self.prediction_column = prediction_column
        self.convert_non_numeric = convert_non_numeric

    def relevant_counter(self, row: pd.core.series.Series, k: int) -> int:
        if self.convert_non_numeric:
            return sum(
                [1 if pred_val in row[self.target_column] else 0 for pred_val in row[self.prediction_column][:k]]
            )
        else:
            paired_sorted = sorted(zip(row[self.prediction_column], row[self.target_column]))
            sorted_predictions, sorted_targets = zip(*paired_sorted)
            sorted_predictions, sorted_targets = list(sorted_predictions), list(sorted_targets)
            return sum([1 if target_val else 0 for target_val in sorted_targets[:k]])

    def sum_gains(self, row: pd.core.series.Series, k: int) -> int:
        if self.convert_non_numeric:
            return sum(
                [1 if pred_val in row[self.target_column] else 0 for pred_val in row[self.prediction_column][:k]]
            )
        else:
            paired_sorted = sorted(zip(row[self.prediction_column], row[self.target_column]))
            sorted_predictions, sorted_targets = zip(*paired_sorted)
            sorted_predictions, sorted_targets = list(sorted_predictions), list(sorted_targets)
            return sum([target_val if target_val else 0 for target_val in sorted_targets[:k]])

    def is_k_item_relevant(self, row: pd.core.series.Series, k: int) -> int:
        if self.convert_non_numeric:
            return 1 if row[self.prediction_column][k - 1] in row[self.target_column] else 0
        else:
            index_ki = row[self.prediction_column].index(k)
            return 1 if row[self.target_column][index_ki] else 0

    def get_top_rank(self, row: pd.core.series.Series, k: int) -> Optional[int]:
        for ki in range(1, k + 1):
            if self.is_k_item_relevant(row, ki):
                return ki
        return None

    def calc_non_numeric_relevance(self, row_dict: pd.core.series.Series) -> Tuple[List[int], List[int]]:
        prediction_relevance = []
        ideal_relevance = []
        for target_val in row_dict[self.prediction_column]:
            ideal_relevance.append(1 if target_val in row_dict[self.target_column] else 0)
            prediction_relevance.append(1 if target_val in row_dict[self.target_column] else 0)
        for target_val in row_dict[self.target_column]:
            if target_val not in row_dict[self.prediction_column]:
                ideal_relevance.append(1)
        return (prediction_relevance, sorted(ideal_relevance, reverse=True))

    def calculate_row_ndcg(self, row_dict: pd.core.series.Series, k: int) -> float:
        if not self.convert_non_numeric:
            dcg_vals = [
                rel / math.log2(pos + 1)
                for rel, pos in zip(row_dict[self.target_column], row_dict[self.prediction_column])
                if pos <= k
            ]
            idcg_vals = [
                rel / math.log2(pos + 2)
                for pos, rel in enumerate(sorted(row_dict[self.target_column], reverse=True)[:k])
            ]
        else:
            predicted_relevances, ideal_relevances = self.calc_non_numeric_relevance(row_dict)
            dcg_vals = [(rel / math.log(i + 2, 2)) for i, rel in enumerate(predicted_relevances[:k])]
            idcg_vals = [(rel / math.log(i + 2, 2)) for i, rel in enumerate(ideal_relevances[:k])]
        if sum(idcg_vals) == 0:
            return 1  # if there is no relevant data, not much the recommender can do
        return sum(dcg_vals) / sum(idcg_vals)


def _calculate_average_precisions(
    formatted_data: pd.core.frame.DataFrame,
    target_column: str,
    prediction_column: str,
    convert_non_numeric: bool,
    k: int,
) -> np.ndarray:
    ki_dict: pd.DataFrame = None
    last_item_relevant_dict: pd.DataFrame = None
    row_metrics_functions = RowWiseMetrics(target_column, prediction_column, convert_non_numeric)

    for ki in range(1, k + 1):
        ki_result = (
            formatted_data.apply(
                row_metrics_functions.relevant_counter,
                args=(ki,),
                axis=1,
            )
            / ki
        )
        last_item_result = formatted_data.apply(row_metrics_functions.is_k_item_relevant, args=(ki,), axis=1)
        if ki == 1:
            ki_dict = ki_result.to_frame()
            ki_dict.columns = ["p@" + str(ki)]
            last_item_relevant_dict = last_item_result.to_frame()
            last_item_relevant_dict.columns = ["last_item_relevant@" + str(ki)]
        else:
            ki_dict["p@" + str(ki)] = ki_result
            last_item_relevant_dict["last_item_relevant@" + str(ki)] = last_item_result
    aps = np.multiply(ki_dict.values, last_item_relevant_dict.values)
    nonzero_counts = np.count_nonzero(aps, axis=1)
    nonzero_counts[nonzero_counts == 0] = 1
    row_sums = aps.sum(axis=1)
    averages = row_sums / nonzero_counts
    return averages


def _all_strings(data: pd.Series) -> bool:
    return all([all([isinstance(y, str) for y in x]) for x in data])


def _get_segment_columns(schema: DatasetSchema, data: pd.DataFrame) -> List[str]:
    columns: Set[str] = set()
    for partition_name, partition in schema.segments.items():
        if partition.filter:
            raise ValueError("Filters are not supported for segmented ranking metrics")  # Filters are deprecated
        if partition.mapper:
            columns = columns.union(set(partition.mapper.col_names))

    return list(columns)


def _drop_non_output_columns(result: SegmentedResultSet, keep_columns: Set[str]) -> SegmentedResultSet:
    for partition in result._segments.values():
        for segment in partition.values():
            for column in {column for column in segment._columns.keys() if column not in keep_columns}:
                segment._columns.pop(column)

    return result


def log_batch_ranking_metrics(
    data: pd.core.frame.DataFrame,
    prediction_column: Optional[str] = None,
    target_column: Optional[str] = None,
    score_column: Optional[str] = None,
    k: Optional[int] = None,
    schema: Union[DatasetSchema, None] = None,
    log_full_data: bool = False,
) -> ViewResultSet:
    """Log ranking metrics for a batch of data.

    You can call the function several ways:
      - Pass both prediction_column and target_column.
          - The named columns contain lists of strings. In this case, the prediction column contains the
            items the model has predicted are relevant, and the target column contains the items that
            are actually relevant. In this case, relevance is boolean.

          - The prediction column contains lists of integers and the target column contains lists of numbers
            or booleans. The value at the i-th position in the predicted list is the predicted rank of the i-th
            element of the domain. The value at the i-th position in the target list is the true relevance score of the
            i-th element of the domain. The score can be numeric or boolean. Higher scores indicate higher relevance.

      - Pass both target_column and score_column. The value at the i-th position in the target list is the true relevance
        of the i-th element of the domain (represented as a number, higher being more relevant; or boolean). The value at
        the i-th position in the score list is the model output for the i-th element of the domain.

      - Pass only target_column. The target column contians lists of numbers or booleans. The list entries are the true
        relevance of the items predicted by the model in prediction order.

    Parameters
    ----------
    data : pd.core.frame.DataFrame
        Dataframe with the data to log.
    prediction_column : Optional[str], optional
        Column name for the predicted values. If not provided, the score_column and target_column must be provided, by default None
    target_column : Optional[str], optional
        Column name for the relevance scores. If not provided, relevance must be encoded within prediction column, by default None
    score_column : Optional[str], optional
        Column name for the scores. Can either be probabilities, confidence values, or other continuous measures.
        If not passed, prediction_column must be passed,by default None
    k : Optional[int], optional
        Consider the top k ranks for metrics calculation.
        If `None`, use all outputs, by default None
    schema : Union[DatasetSchema, None], optional
        Defines the schema for tracking metrics in whylogs, by default None
    log_full_data : bool, optional
        Whether to log the complete dataframe or not.
        If True, the complete DF will be logged in addition to the ranking metrics.
        If False, only the calculated ranking metrics will be logged.
        In a typical production use case, the ground truth might not be available
        at the time the remaining data is generated. In order to prevent double profiling the
        input features, consider leaving this as False. by default False

    Returns
    -------
    ViewResultSet

    Examples
    --------
    ::

        import pandas as pd
        from whylogs.experimental.api.logger import log_batch_ranking_metrics

        # 1st and 2nd recommended items are relevant - 3rd is not
        df = pd.DataFrame({"targets": [[1, 0, 1]], "predictions": [[2,3,1]]})
        results = log_batch_ranking_metrics(
            data=df,
            prediction_column="predictions",
            target_column="targets",
            k=3,
        )

    ::

        non_numerical_df = pd.DataFrame(
            {
                "raw_predictions": [
                    ["cat", "pig", "elephant"],
                    ["horse", "donkey", "robin"],
                ],
                "raw_targets": [
                    ["cat", "elephant"],
                    ["dog"],
                ],
            }
        )

        # 1st query:
        # Recommended items: [cat, pig, elephant]
        # Relevant items: [cat, elephant]


        # 2nd query:
        # Recommended items: [horse, donkey, robin]
        # Relevant items: [dog]

        results = log_batch_ranking_metrics(
            k=2,
            data=non_numerical_df,
            prediction_column="raw_predictions",
            target_column="raw_targets",
            convert_non_numeric=True
        )

    ::

        binary_single_df = pd.DataFrame(
            {
                "raw_targets": [
                    [True, False, True], # First recommended item: Relevant, Second: Not relevant, Third: Relevant
                    [False, False, False], # None of the recommended items are relevant
                    [True, True, False], # First and second recommended items are relevant
                ]
            }
        )

        result = log_batch_ranking_metrics(data=binary_single_df, target_column="raw_targets", k=3)

    """
    formatted_data = data.copy(deep=True)  # TODO: does this have to be deep?

    if score_column is not None and prediction_column is not None:
        raise ValueError("Cannot specify both score_column and prediction_column")

    if prediction_column is None and score_column is None and target_column is not None:
        # https://github.com/whylabs/whylogs/issues/1505
        # The column use logic is complex, so just swapping them here for this case
        # rather than unraveling all the use cases.
        prediction_column, target_column = target_column, prediction_column

    if prediction_column is None:
        if score_column is not None and target_column is not None:
            prediction_column = "__predictions"

            # Ties are not being handled here
            formatted_data[prediction_column] = formatted_data[score_column].apply(
                lambda row: list(np.argsort(np.argsort(-np.array(row))) + 1)
            )
        else:
            raise ValueError("Either target_column or score+target columns must be specified")

    relevant_cols = [prediction_column]

    if target_column is None:
        formatted_data = _convert_to_int_if_bool(formatted_data, prediction_column)
        target_column = "__targets"
        # the relevances in predicitons are moved to targets, and predicitons contains the indices to the target list
        formatted_data[target_column] = formatted_data[prediction_column]
        formatted_data[prediction_column] = formatted_data[target_column].apply(
            lambda row: list(range(1, len(row) + 1))
        )

    relevant_cols.append(target_column)
    if score_column is not None:
        relevant_cols.append(score_column)
    for col in relevant_cols:
        if not formatted_data[col].apply(lambda x: type(x) == list).all():
            # wrapping in lists because at least one isn't a list
            # TODO: more error checking
            formatted_data[col] = formatted_data[col].apply(lambda x: [x])
    _max_k = formatted_data[prediction_column].apply(len).max()
    if not k:
        k = _max_k
    if k > _max_k:
        diagnostic_logger.warning(
            f"Max value of k in the dataset is {_max_k}, but k was set to {k}. Setting k to {_max_k}"
        )
        k = _max_k
    if k and k < 1:
        raise ValueError("k must be a positive integer")

    convert_non_numeric = _all_strings(formatted_data[prediction_column]) and _all_strings(
        formatted_data[target_column]
    )

    row_wise_functions = RowWiseMetrics(target_column, prediction_column, convert_non_numeric)
    formatted_data["count_at_k"] = formatted_data.apply(row_wise_functions.relevant_counter, args=(k,), axis=1)
    formatted_data["count_all"] = formatted_data.apply(row_wise_functions.relevant_counter, args=(_max_k,), axis=1)
    formatted_data["top_rank"] = formatted_data[relevant_cols].apply(
        row_wise_functions.get_top_rank, args=(_max_k,), axis=1
    )

    output_data = pd.DataFrame()
    output_data[f"recall_k_{k}"] = formatted_data["count_at_k"] / formatted_data["count_all"]
    output_data[f"precision_k_{k}"] = formatted_data["count_at_k"] / (k if k else 1)
    output_data["top_rank"] = formatted_data["top_rank"]
    output_data["average_precision" + ("_k_" + str(k) if k else "")] = _calculate_average_precisions(
        formatted_data, target_column, prediction_column, convert_non_numeric=convert_non_numeric, k=k  # type: ignore
    )

    output_data["norm_dis_cumul_gain" + ("_k_" + str(k) if k else "")] = formatted_data.apply(
        row_wise_functions.calculate_row_ndcg, args=(k,), axis=1
    )
    output_data[f"sum_gain_k_{k}"] = formatted_data.apply(row_wise_functions.sum_gains, args=(k,), axis=1)
    mrr = (1 / formatted_data["top_rank"]).replace([np.inf, np.nan], 0)
    output_data["reciprocal_rank"] = mrr

    if schema and schema.segments:
        original_columns = set(data.columns)
        for column in set(formatted_data.columns):
            if column not in original_columns:
                formatted_data = formatted_data.drop(column, axis=1)

        if log_full_data:
            return log(pandas=pd.concat([formatted_data, output_data], axis=1), schema=schema)
        else:
            segment_columns = _get_segment_columns(schema, formatted_data)
            segmentable_data = formatted_data[segment_columns]
            result = log(pandas=pd.concat([segmentable_data, output_data], axis=1), schema=schema)
            result = _drop_non_output_columns(result, set(output_data.columns))
            return result

    result = log(pandas=output_data, schema=schema)
    if log_full_data:
        result = result.merge(log(pandas=data, schema=schema))
    return result
