from math import isclose

from whylogs.core.stubs import pd
from whylogs.experimental.api.logger import log_batch_ranking_metrics


def test_log_batch_ranking_metrics_single_simple():
    single_df = pd.DataFrame(
        {
            "raw_predictions": [
                ["cat", "pig", "elephant"],
                ["horse", "donkey", "robin"],
                ["cow", "pig", "giraffe"],
                ["pig", "dolphin", "elephant"],
            ],
            "raw_targets": ["cat", "dog", "pig", "elephant"],
        }
    )
    result = log_batch_ranking_metrics(
        data=single_df, prediction_column="raw_predictions", target_column="raw_targets", convert_non_numeric=True
    )
    pandas_summary = result.view().to_pandas()

    column_names = [
        "mean_average_precision",
        "accuracy",
        "mean_reciprocal_rank",
        "precision",
        "recall",
        "top_rank",
        "average_precision",
        "norm_dis_cumul_gain",
    ]
    for col in column_names:
        assert col in pandas_summary.index
    assert pandas_summary.loc["mean_average_precision", "counts/n"] == 1
    assert pandas_summary.loc["accuracy", "counts/n"] == 1
    assert pandas_summary.loc["mean_reciprocal_rank", "counts/n"] == 1
    assert pandas_summary.loc["precision", "counts/n"] == 4
    assert pandas_summary.loc["recall", "counts/n"] == 4
    assert pandas_summary.loc["top_rank", "counts/n"] == 4
    assert pandas_summary.loc["average_precision", "counts/n"] == 4
    assert pandas_summary.loc["norm_dis_cumul_gain", "counts/n"] == 1
    assert pandas_summary.loc["average_precision", "counts/n"] == 4
    assert pandas_summary.loc["norm_dis_cumul_gain", "counts/n"] == 1


def test_log_batch_ranking_metrics_binary_simple():
    binary_df = pd.DataFrame(
        {"raw_predictions": [[True, False, True], [False, False, False], [True, True, False], [False, True, False]]}
    )

    result = log_batch_ranking_metrics(
        data=binary_df, prediction_column="raw_predictions", k=2, convert_non_numeric=True
    )
    pandas_summary = result.view().to_pandas()

    k = 2
    column_names = [
        "mean_average_precision_k_" + str(k),
        "accuracy_k_" + str(k),
        "mean_reciprocal_rank",
        "precision_k_" + str(k),
        "recall_k_" + str(k),
        "top_rank",
        "average_precision_k_" + str(k),
        "norm_dis_cumul_gain_k_" + str(k),
    ]
    for col in column_names:
        assert col in pandas_summary.index
    assert pandas_summary.loc["mean_average_precision_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["accuracy_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["mean_reciprocal_rank", "counts/n"] == 1
    assert pandas_summary.loc["precision_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["recall_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["top_rank", "counts/n"] == 4
    assert pandas_summary.loc["average_precision_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["norm_dis_cumul_gain_k_" + str(k), "counts/n"] == 1


def test_log_batch_ranking_metrics_multiple_simple():
    multiple_df = pd.DataFrame(
        {
            "raw_targets": [["cat", "elephant"], ["dog", "pig"], ["pig", "cow"], ["cat", "dolphin"]],
            "raw_predictions": [
                ["cat", "pig", "elephant"],
                ["horse", "donkey", "robin"],
                ["cow", "pig", "giraffe"],
                ["pig", "dolphin", "elephant"],
            ],
        }
    )
    k = 4

    result = log_batch_ranking_metrics(
        data=multiple_df,
        prediction_column="raw_predictions",
        target_column="raw_targets",
        k=k,
        convert_non_numeric=True,
    )
    pandas_summary = result.view().to_pandas()

    column_names = [
        "mean_average_precision_k_" + str(k),
        "accuracy_k_" + str(k),
        "mean_reciprocal_rank",
        "precision_k_" + str(k),
        "recall_k_" + str(k),
        "top_rank",
        "average_precision_k_" + str(k),
        "norm_dis_cumul_gain_k_" + str(k),
    ]
    for col in column_names:
        assert col in pandas_summary.index
    assert pandas_summary.loc["mean_average_precision_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["accuracy_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["mean_reciprocal_rank", "counts/n"] == 1
    assert pandas_summary.loc["precision_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["recall_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["top_rank", "counts/n"] == 4
    assert pandas_summary.loc["average_precision_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["norm_dis_cumul_gain_k_" + str(k), "counts/n"] == 1

    assert isclose(pandas_summary.loc[f"norm_dis_cumul_gain_k_{k}", "distribution/median"], 0.76244, abs_tol=0.00001)


def test_log_batch_ranking_metrics_default_target():
    multiple_df = pd.DataFrame({"raw_predictions": [[3, 2, 3, 0, 1, 2, 3, 2]]})

    result = log_batch_ranking_metrics(
        data=multiple_df, prediction_column="raw_predictions", k=3, convert_non_numeric=True
    )
    pandas_summary = result.view().to_pandas()

    k = 3
    column_names = [
        "mean_average_precision_k_" + str(k),
        "accuracy_k_" + str(k),
        "mean_reciprocal_rank",
        "precision_k_" + str(k),
        "recall_k_" + str(k),
        "top_rank",
        "average_precision_k_" + str(k),
        "norm_dis_cumul_gain_k_" + str(k),
    ]
    for col in column_names:
        assert col in pandas_summary.index
    assert pandas_summary.loc["mean_average_precision_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["accuracy_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["mean_reciprocal_rank", "counts/n"] == 1
    assert pandas_summary.loc["precision_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["recall_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["top_rank", "counts/n"] == 1
    assert pandas_summary.loc["average_precision_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["norm_dis_cumul_gain_k_" + str(k), "counts/n"] == 1


def test_log_batch_ranking_metrics_ranking_ndcg_wikipedia():
    # From https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Example
    ranking_df = pd.DataFrame({"targets": [[3, 2, 3, 0, 1, 2, 3, 2]], "predictions": [[7, 6, 5, 4, 3, 2, 1, 0]]})

    result = log_batch_ranking_metrics(data=ranking_df, prediction_column="predictions", target_column="targets", k=6)
    pandas_summary = result.view().to_pandas()

    assert isclose(pandas_summary.loc["norm_dis_cumul_gain_k_6", "distribution/median"], 0.785, abs_tol=0.01)


def test_log_batch_ranking_metrics_ranking_ndcg_sklearn():
    # From https://scikit-learn.org/stable/modules/generated/sklearn.metrics.ndcg_score.html
    ranking_df = pd.DataFrame({"predictions": [[0.1, 0.2, 0.3, 4, 70]], "targets": [[10, 0, 0, 1, 5]]})

    result = log_batch_ranking_metrics(data=ranking_df, prediction_column="predictions", target_column="targets")
    pandas_summary = result.view().to_pandas()

    assert isclose(pandas_summary.loc["norm_dis_cumul_gain", "distribution/median"], 0.69569, abs_tol=0.00001)


def test_log_batch_ranking_metrics_ranking_ndcg_withk_sklearn():
    # From https://scikit-learn.org/stable/modules/generated/sklearn.metrics.ndcg_score.html
    ranking_df = pd.DataFrame({"predictions": [[0.05, 1.1, 1.0, 0.5, 0.0]], "targets": [[10, 0, 0, 1, 5]]})

    result = log_batch_ranking_metrics(data=ranking_df, prediction_column="predictions", target_column="targets", k=4)
    pandas_summary = result.view().to_pandas()

    assert isclose(pandas_summary.loc["norm_dis_cumul_gain_k_4", "distribution/median"], 0.35202, abs_tol=0.00001)
