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
        "accuracy_k_3",
        "reciprocal_rank",
        "precision_k_3",
        "recall_k_3",
        "top_rank",
        "average_precision_k_3",
        "norm_dis_cumul_gain_k_3",
    ]
    for col in column_names:
        assert col in pandas_summary.index
    assert pandas_summary.loc["accuracy_k_3", "counts/n"] == 1
    assert pandas_summary.loc["reciprocal_rank", "counts/n"] == 4
    assert pandas_summary.loc["precision_k_3", "counts/n"] == 4
    assert pandas_summary.loc["recall_k_3", "counts/n"] == 4
    assert pandas_summary.loc["top_rank", "counts/n"] == 4
    assert pandas_summary.loc["average_precision_k_3", "counts/n"] == 4
    assert pandas_summary.loc["average_precision_k_3", "counts/n"] == 4
    assert pandas_summary.loc["norm_dis_cumul_gain_k_3", "counts/n"] == 4
    # ndcg = [1, 0, 0.63, 0.5]
    assert isclose(pandas_summary.loc["norm_dis_cumul_gain_k_3", "distribution/mean"], 0.53273, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["average_precision_k_3", "distribution/mean"], 0.45833, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["precision_k_3", "distribution/mean"], 0.25, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["recall_k_3", "distribution/mean"], 1.0, abs_tol=0.00001)
    # rr = [1, 0, 0.5, 0.33333]
    assert isclose(pandas_summary.loc["reciprocal_rank", "distribution/mean"], 0.45833, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["accuracy_k_3", "distribution/mean"], 0.75, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["sum_gain_k_3", "distribution/mean"], 0.75, abs_tol=0.00001)


def test_log_batch_ranking_metrics_binary_simple():
    binary_df = pd.DataFrame(
        {"raw_predictions": [[True, False, True], [False, False, False], [True, True, False], [False, True, False]]}
    )

    result = log_batch_ranking_metrics(data=binary_df, prediction_column="raw_predictions", k=2)
    pandas_summary = result.view().to_pandas()

    k = 2
    column_names = [
        "accuracy_k_" + str(k),
        "reciprocal_rank",
        "precision_k_" + str(k),
        "recall_k_" + str(k),
        "top_rank",
        "average_precision_k_" + str(k),
        "norm_dis_cumul_gain_k_" + str(k),
    ]
    for col in column_names:
        assert col in pandas_summary.index
    assert pandas_summary.loc["accuracy_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["reciprocal_rank", "counts/n"] == 4
    assert pandas_summary.loc["precision_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["recall_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["top_rank", "counts/n"] == 4
    assert pandas_summary.loc["average_precision_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["norm_dis_cumul_gain_k_" + str(k), "counts/n"] == 4
    # ndcg@2 = [0.613147, 1.0, 1.0, 0.63093]
    # average_precision_k_2 = [1.0, 0.0, 1.0, 0.5]
    assert isclose(pandas_summary.loc["norm_dis_cumul_gain_k_" + str(k), "distribution/mean"], 0.81101, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["average_precision_k_" + str(k), "distribution/mean"], 0.62500, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["precision_k_" + str(k), "distribution/mean"], 0.5, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["recall_k_" + str(k), "distribution/mean"], 0.83333, abs_tol=0.00001)
    # rr = [1, 0, 1, 0.5]
    assert isclose(pandas_summary.loc["reciprocal_rank", "distribution/mean"], 0.625, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["accuracy_k_2", "distribution/mean"], 0.75, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["sum_gain_k_2", "distribution/mean"], 1.0, abs_tol=0.00001)


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
    k = 3

    result = log_batch_ranking_metrics(
        data=multiple_df,
        prediction_column="raw_predictions",
        target_column="raw_targets",
        k=k,
        convert_non_numeric=True,
    )
    pandas_summary = result.view().to_pandas()

    column_names = [
        "accuracy_k_" + str(k),
        "reciprocal_rank",
        "precision_k_" + str(k),
        "recall_k_" + str(k),
        "top_rank",
        "average_precision_k_" + str(k),
        "norm_dis_cumul_gain_k_" + str(k),
    ]
    for col in column_names:
        assert col in pandas_summary.index
    assert pandas_summary.loc["accuracy_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["reciprocal_rank", "counts/n"] == 4
    assert pandas_summary.loc["precision_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["recall_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["top_rank", "counts/n"] == 4
    assert pandas_summary.loc["average_precision_k_" + str(k), "counts/n"] == 4
    assert pandas_summary.loc["norm_dis_cumul_gain_k_" + str(k), "counts/n"] == 4
    # ndcg@3 = [0.9197, 0.0, 1.0, 0.386853]
    # average_precision_k_3 = [0.83, 0.0, 1.0, 0.5]
    assert isclose(pandas_summary.loc[f"norm_dis_cumul_gain_k_{k}", "distribution/mean"], 0.57664, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["average_precision_k_" + str(k), "distribution/mean"], 0.58333, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["sum_gain_k_" + str(k), "distribution/mean"], 1.25, abs_tol=0.00001)


def test_log_batch_ranking_metrics_default_target():
    multiple_df = pd.DataFrame({"raw_predictions": [[3, 2, 3, 0, 1, 2, 3, 2]]})

    result = log_batch_ranking_metrics(data=multiple_df, prediction_column="raw_predictions", k=3)
    pandas_summary = result.view().to_pandas()

    k = 3
    column_names = [
        "accuracy_k_" + str(k),
        "reciprocal_rank",
        "precision_k_" + str(k),
        "recall_k_" + str(k),
        "top_rank",
        "average_precision_k_" + str(k),
        "norm_dis_cumul_gain_k_" + str(k),
    ]
    for col in column_names:
        assert col in pandas_summary.index
    assert pandas_summary.loc["accuracy_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["reciprocal_rank", "counts/n"] == 1
    assert pandas_summary.loc["precision_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["recall_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["top_rank", "counts/n"] == 1
    assert pandas_summary.loc["average_precision_k_" + str(k), "counts/n"] == 1
    assert pandas_summary.loc["norm_dis_cumul_gain_k_" + str(k), "counts/n"] == 1
    # ndcg@3 = [0.9013]
    assert isclose(pandas_summary.loc[f"norm_dis_cumul_gain_k_{k}", "distribution/median"], 0.90130, abs_tol=0.00001)
    # AP assumes binary relevance - this case doesn't raise an error, just a warning, but the result is not meaningful
    assert isclose(pandas_summary.loc["average_precision_k_" + str(k), "distribution/mean"], 1.00000, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["accuracy_k_3", "distribution/mean"], 1.0, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["sum_gain_k_3", "distribution/mean"], 8.0, abs_tol=0.00001)


def test_log_batch_ranking_metrics_ranking_ndcg_wikipedia():
    # From https://en.wikipedia.org/wiki/Discounted_cumulative_gain#Example
    ranking_df = pd.DataFrame({"targets": [[1, 0, 2, 3, 3, 2, 2, 3]], "predictions": [[5, 4, 2, 1, 7, 8, 6, 3]]})

    result = log_batch_ranking_metrics(data=ranking_df, prediction_column="predictions", target_column="targets", k=6)
    pandas_summary = result.view().to_pandas()

    assert isclose(pandas_summary.loc["norm_dis_cumul_gain_k_6", "distribution/median"], 0.785, abs_tol=0.01)


def test_log_batch_ranking_metrics_ranking_ndcg_sklearn():
    # From https://scikit-learn.org/stable/modules/generated/sklearn.metrics.ndcg_score.html
    ranking_df = pd.DataFrame({"scores": [[0.1, 0.2, 0.3, 4, 70]], "true_relevance": [[10, 0, 0, 1, 5]]})

    result = log_batch_ranking_metrics(data=ranking_df, score_column="scores", target_column="true_relevance")
    pandas_summary = result.view().to_pandas()

    assert isclose(pandas_summary.loc["norm_dis_cumul_gain_k_5", "distribution/median"], 0.69569, abs_tol=0.00001)


def test_log_batch_ranking_metrics_ranking_ndcg_withk_sklearn():
    # From https://scikit-learn.org/stable/modules/generated/sklearn.metrics.ndcg_score.html
    ranking_df = pd.DataFrame({"scores": [[0.05, 1.1, 1.0, 0.5, 0.0]], "true_relevance": [[10, 0, 0, 1, 5]]})

    result = log_batch_ranking_metrics(data=ranking_df, score_column="scores", target_column="true_relevance", k=4)
    pandas_summary = result.view().to_pandas()

    assert isclose(pandas_summary.loc["norm_dis_cumul_gain_k_4", "distribution/median"], 0.35202, abs_tol=0.00001)


def test_log_batch_ranking_metrics_average_precision_sklearn_example():
    # from https://scikit-learn.org/stable/modules/generated/sklearn.metrics.average_precision_score.html
    k = 4
    ranking_df = pd.DataFrame({"scores": [[0.1, 0.4, 0.35, 0.8]], "true_relevance": [[0, 0, 1, 1]]})
    result = log_batch_ranking_metrics(data=ranking_df, score_column="scores", target_column="true_relevance", k=k)
    pandas_summary = result.view().to_pandas()

    assert isclose(pandas_summary.loc["average_precision_k_" + str(k), "distribution/mean"], 0.83333, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["precision_k_" + str(k), "distribution/mean"], 0.5, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["recall_k_" + str(k), "distribution/mean"], 1.0, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["reciprocal_rank", "distribution/mean"], 1.0, abs_tol=0.00001)
    assert isclose(pandas_summary.loc["sum_gain_k_" + str(k), "distribution/mean"], 2.0, abs_tol=0.00001)


def test_log_batch_ranking_metrics_average_precision():
    expected_results = [(1, 0.25), (2, 0.375), (3, 0.45833)]
    for res in expected_results:
        k = res[0]
        ranking_df = pd.DataFrame(
            {
                "targets": [[1, 0, 1], [0, 0, 1], [0, 0, 1], [0, 0, 0]],
                "predictions": [[2, 3, 1], [1, 3, 2], [1, 2, 3], [3, 1, 2]],
            }
        )
        result = log_batch_ranking_metrics(
            data=ranking_df, target_column="targets", prediction_column="predictions", k=k
        )
        pandas_summary = result.view().to_pandas()

        assert isclose(
            pandas_summary.loc["average_precision_k_" + str(k), "distribution/mean"], res[1], abs_tol=0.00001
        )
        assert isclose(pandas_summary.loc["reciprocal_rank", "distribution/mean"], 0.45833, abs_tol=0.00001)
