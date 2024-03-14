import logging
import math
from typing import Optional, Union

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

    def _calc_non_numeric_relevance(row_dict):
        prediction_relevance = []
        ideal_relevance = []
        for target_val in row_dict[prediction_column]:
            ideal_relevance.append(1 if target_val in row_dict[target_column] else 0)
            prediction_relevance.append(1 if target_val in row_dict[target_column] else 0)
        for target_val in row_dict[target_column]:
            if target_val not in row_dict[prediction_column]:
                ideal_relevance.append(1)
        return (prediction_relevance, sorted(ideal_relevance, reverse=True))

    def _calculate_row_ndcg(row_dict, k):
        if not convert_non_numeric:
            dcg_vals = [
                rel / math.log2(pos + 1)
                for rel, pos in zip(row_dict[target_column], row_dict[prediction_column])
                if pos <= k
            ]
            idcg_vals = [
                rel / math.log2(pos + 2) for pos, rel in enumerate(sorted(row_dict[target_column], reverse=True)[:k])
            ]
        else:
            predicted_relevances, ideal_relevances = _calc_non_numeric_relevance(row_dict)
            dcg_vals = [(rel / math.log(i + 2, 2)) for i, rel in enumerate(predicted_relevances[:k])]
            idcg_vals = [(rel / math.log(i + 2, 2)) for i, rel in enumerate(ideal_relevances[:k])]
        if sum(idcg_vals) == 0:
            return 1  # if there is no relevant data, not much the recommender can do
        return sum(dcg_vals) / sum(idcg_vals)

    formatted_data["norm_dis_cumul_gain" + ("_k_" + str(k) if k else "")] = formatted_data.apply(
        _calculate_row_ndcg, args=(k,), axis=1
    )
    mAP_at_k = ki_dict.mean()
    hit_ratio = formatted_data["count_at_k"].apply(lambda x: bool(x)).sum() / len(formatted_data)
    mrr = (1 / formatted_data["top_rank"]).replace([np.inf], np.nan).mean()
    ndcg = formatted_data["norm_dis_cumul_gain" + ("_k_" + str(k) if k else "")].mean()
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
