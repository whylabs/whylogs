import logging
import math
from typing import List, Optional, Tuple, Union

from whylogs.api.logger import log
from whylogs.api.logger.result_set import ViewResultSet
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


def log_batch_ranking_metrics(
    data: pd.core.frame.DataFrame,
    prediction_column: Optional[str] = None,
    target_column: Optional[str] = None,
    score_column: Optional[str] = None,
    k: Optional[int] = None,
    convert_non_numeric=False,
    schema: Union[DatasetSchema, None] = None,
    log_full_data: bool = False,
) -> ViewResultSet:
    formatted_data = data.copy(deep=True)  # TODO: does this have to be deep?

    if prediction_column is None:
        if score_column is not None and target_column is not None:
            prediction_column = "__predictions"

            # Ties are not being handled here
            formatted_data[prediction_column] = formatted_data[score_column].apply(
                lambda row: list(np.argsort(np.argsort(-np.array(row))) + 1)
            )
        else:
            raise ValueError("Either prediction_column or score+target columns must be specified")

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
    hit_ratio = formatted_data["count_at_k"].apply(lambda x: bool(x)).sum() / len(formatted_data)
    mrr = (1 / formatted_data["top_rank"]).replace([np.inf, np.nan], 0)
    output_data["reciprocal_rank"] = mrr
    result = log(pandas=output_data, schema=schema)
    result = result.merge(
        log(
            row={
                "accuracy" + ("_k_" + str(k) if k else ""): hit_ratio,
            },
            schema=schema,
        )
    )
    if log_full_data:
        result = result.merge(log(pandas=data, schema=schema))
    return result
