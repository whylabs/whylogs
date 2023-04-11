import logging
from typing import Optional, Union

from whylogs.api.logger import log
from whylogs.api.logger.result_set import ProfileResultSet
from whylogs.core import DatasetSchema
from whylogs.core.stubs import np, pd

diagnostic_logger = logging.getLogger(__name__)


def log_batch_ranking_metrics(
    k: int,
    data: pd.core.frame.DataFrame,
    prediction_column: str,
    target_column: Optional[str] = None,
    score_column: Optional[str] = None,
    schema: Union[DatasetSchema, None] = None,
    log_full_data: bool = False,
) -> ProfileResultSet:
    formatted_data = data.copy(deep=True)  # TODO: does this have to be deep?

    relevant_cols = [prediction_column]
    if target_column is None:
        target_column = "__targets"
        formatted_data[target_column] = True
        formatted_data[target_column].apply(lambda x: [x])
    relevant_cols.append(target_column)
    if score_column is not None:
        relevant_cols.append(score_column)

    for col in relevant_cols:
        if not formatted_data[col].apply(lambda x: type(x) == list).all():
            # wrapping in lists because at least one isn't a list
            # TODO: more error checking
            formatted_data[col] = formatted_data[col].apply(lambda x: [x])

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

    output_data = (formatted_data["count_at_k"] / k).to_frame()
    output_data.columns = ["precision_k_" + str(k)]
    output_data["recall_k_" + str(k)] = formatted_data["count_at_k"] / formatted_data["count_all"]
    output_data["top_rank"] = formatted_data["top_rank"]

    ki_dict: pd.DataFrame = None
    for ki in range(1, k + 1):
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

    output_data["average_precision_k_" + str(k)] = ki_dict.mean(axis=1)
    mAP_at_k = output_data["average_precision_k_" + str(k)].mean(axis=0)
    hit_ratio = formatted_data["count_at_k"].apply(lambda x: bool(x)).sum() / len(formatted_data)
    mrr = (1 / output_data["top_rank"]).replace([np.inf], np.nan).mean()

    result = log(pandas=output_data, schema=schema)
    result = result.merge(
        log(
            row={
                "mean_average_precision_k_" + str(k): mAP_at_k,
                "accuracy_k_" + str(k): hit_ratio,
                "mean_reciprocal_rank": mrr,
            },
            schema=schema,
        )
    )

    if log_full_data:
        result = result.merge(log(pandas=data, schema=schema))

    return result
