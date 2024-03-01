import logging
import math
from typing import Optional, Union

from whylogs.api.logger import log
from whylogs.api.logger.result_set import ViewResultSet
from whylogs.core import DatasetSchema
from whylogs.core.stubs import np, pd

diagnostic_logger = logging.getLogger(__name__)


def log_batch_ranking_metrics(
    data: pd.core.frame.DataFrame,
    prediction_column: str,
    target_column: Optional[str] = None,
    score_column: Optional[str] = None,
    k: Optional[int] = None,
    convert_non_numeric=False,
    schema: Union[DatasetSchema, None] = None,
    log_full_data: bool = False,
) -> ViewResultSet:
    formatted_data = data.copy(deep=True)  # TODO: does this have to be deep?

    relevant_cols = [prediction_column]
    if target_column is None:
        target_column = "__targets"
        formatted_data[target_column] = formatted_data[prediction_column].apply(lambda x: list(range(len(x)))[::-1])
    relevant_cols.append(target_column)
    if score_column is not None:
        relevant_cols.append(score_column)

    for col in relevant_cols:
        if not formatted_data[col].apply(lambda x: type(x) == list).all():
            # wrapping in lists because at least one isn't a list
            # TODO: more error checking
            formatted_data[col] = formatted_data[col].apply(lambda x: [x])

    _max_k = formatted_data[prediction_column].apply(len).max()

    formatted_data["count_at_k"] = formatted_data[relevant_cols].apply(
        lambda row: sum([1 if pred_val in row[target_column] else 0 for pred_val in row[prediction_column][:k]]), axis=1
    )

    formatted_data["count_all"] = formatted_data[relevant_cols].apply(
        lambda row: sum([1 if pred_val in row[target_column] else 0 for pred_val in row[prediction_column]]), axis=1
    )

    def get_top_rank(row):
        matches = [i + 1 for i, pred_val in enumerate(row[prediction_column]) if pred_val in row[target_column]]
        if not matches:
            return 0
        else:
            return matches[0]

    formatted_data["top_rank"] = formatted_data[relevant_cols].apply(get_top_rank, axis=1)

    output_data = (formatted_data["count_at_k"] / (k if k else 1)).to_frame()
    output_data.columns = ["precision" + ("_k_" + str(k) if k else "")]
    output_data["recall" + ("_k_" + str(k) if k else "")] = formatted_data["count_at_k"] / formatted_data["count_all"]
    output_data["top_rank"] = formatted_data["top_rank"]

    ki_dict: pd.DataFrame = None
    for ki in range(1, (k if k else _max_k) + 1):
        ki_result = (
            formatted_data[relevant_cols].apply(
                lambda row: sum(
                    [1 if pred_val in row[target_column] else 0 for pred_val in row[prediction_column][:ki]]
                ),
                axis=1,
            )
            / ki
        )
        if ki == 1:
            ki_dict = ki_result.to_frame()
            ki_dict.columns = ["p@" + str(ki)]
        else:
            ki_dict["p@" + str(ki)] = ki_result

    output_data["average_precision" + ("_k_" + str(k) if k else "")] = ki_dict.mean(axis=1)

    def _convert_non_numeric(row_dict):
        return (
            [
                row_dict[target_column].index(pred_val) if pred_val in row_dict[target_column] else -1
                for pred_val in row_dict[prediction_column]
            ],
            list(range(len(row_dict[prediction_column])))[::-1],
        )

    if convert_non_numeric:
        formatted_data[[prediction_column, target_column]] = formatted_data.apply(
            _convert_non_numeric, result_type="expand", axis=1
        )

    def _calculate_row_ndcg(row_dict, k):
        predicted_order = np.array(row_dict[prediction_column]).argsort()[::-1]
        target_order = np.array(row_dict[target_column]).argsort()[::-1]
        dcg_vals = [
            (rel / math.log(i + 2, 2)) for i, rel in enumerate(np.array(row_dict[target_column])[predicted_order][:k])
        ]
        idcg_vals = [
            (rel / math.log(i + 2, 2)) for i, rel in enumerate(np.array(row_dict[target_column])[target_order][:k])
        ]
        return sum(dcg_vals) / sum(idcg_vals)

    formatted_data["norm_dis_cumul_gain_k_" + str(k)] = formatted_data.apply(_calculate_row_ndcg, args=(k,), axis=1)

    mAP_at_k = ki_dict.mean()
    hit_ratio = formatted_data["count_at_k"].apply(lambda x: bool(x)).sum() / len(formatted_data)
    mrr = (1 / formatted_data["top_rank"]).replace([np.inf], np.nan).mean()
    ndcg = formatted_data["norm_dis_cumul_gain_k_" + str(k)].mean()

    result = log(pandas=output_data, schema=schema)
    result = result.merge(
        log(
            row={
                "mean_average_precision" + ("_k_" + str(k) if k else ""): mAP_at_k,
                "accuracy" + ("_k_" + str(k) if k else ""): hit_ratio,
                "mean_reciprocal_rank": mrr,
                "norm_dis_cumul_gain" + ("_k_" + str(k) if k else ""): ndcg,
            },
            schema=schema,
        )
    )

    if log_full_data:
        result = result.merge(log(pandas=data, schema=schema))

    return result
