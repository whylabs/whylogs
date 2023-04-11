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
        k=1, data=single_df, prediction_column="raw_predictions", target_column="raw_targets"
    )
    pandas_summary = result.view().to_pandas()

    k = 1
    column_names = [
        "mean_average_precision_k_" + str(k),
        "accuracy_k_" + str(k),
        "mean_reciprocal_rank",
        "precision_k_" + str(k),
        "recall_k_" + str(k),
        "top_rank",
        "average_precision_k_" + str(k),
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


def test_log_batch_ranking_metrics_binary_simple():
    binary_df = pd.DataFrame(
        {"raw_predictions": [[True, False, True], [False, False, False], [True, True, False], [False, True, False]]}
    )

    result = log_batch_ranking_metrics(k=2, data=binary_df, prediction_column="raw_predictions")
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

    result = log_batch_ranking_metrics(
        k=3, data=multiple_df, prediction_column="raw_predictions", target_column="raw_targets"
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
