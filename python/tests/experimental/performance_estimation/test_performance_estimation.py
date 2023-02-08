import pytest
import pandas as pd
import whylogs as why
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.schema import DatasetSchema
from whylogs.experimental.performance_estimation import AccuracyEstimator
from whylogs.core.segmentation_partition import (
    ColumnMapperFunction,
    SegmentationPartition,
)


@pytest.fixture
def acc_estimator(reference_df):
    reference_result = make_result_set(reference_df)
    acc_estimator = AccuracyEstimator(reference_result)
    return acc_estimator


@pytest.fixture
def reference_df():
    data = {
        "product": ["milk", "carrot", "cheese", "broccoli"],
        "category": ["dairies", "vegetables", "dairies", "vegetables"],
        "output_discount": [0, 0, 1, 1],
        "output_prediction": [0, 0, 0, 1],
    }
    df = pd.DataFrame(data)
    return df


def make_result_set(df, on_column="category", segment=True):
    if isinstance(on_column, str):
        segment_column = on_column
        if segment:
            segmented_schema = DatasetSchema(segments=segment_on_column(segment_column))
        else:
            segmented_schema = None
    elif isinstance(on_column, list):
        segmentation_partition = SegmentationPartition(
            name="multikeys", mapper=ColumnMapperFunction(col_names=on_column)
        )
        multi_column_segments = {segmentation_partition.name: segmentation_partition}
        segmented_schema = DatasetSchema(segments=multi_column_segments)

    results = why.log_classification_metrics(
        df,
        target_column="output_discount",
        prediction_column="output_prediction",
        schema=segmented_schema,
        log_full_data=True,
    )
    return results


def make_regression_result_set(df, on_column="category"):
    segment_column = on_column
    segmented_schema = DatasetSchema(segments=segment_on_column(segment_column))

    results = why.log_regression_metrics(
        df,
        target_column="output_discount",
        prediction_column="output_prediction",
        schema=segmented_schema,
        log_full_data=True,
    )
    return results


@pytest.fixture
def target_df():
    data = {
        "product": ["milk", "creamcheese", "cheese", "broccoli"],
        "category": ["dairies", "dairies", "dairies", "vegetables"],
        "output_discount": [0, 0, 1, 1],
        "output_prediction": [0, 0, 0, 1],
    }
    df = pd.DataFrame(data)
    return df


def test_with_unsegmented_results(acc_estimator, target_df):
    target_results = make_result_set(target_df, segment=False)
    with pytest.raises(AssertionError, match="target must be a SegmentedResultSet"):
        acc_estimator.estimate(target_results)


def test_with_wrong_segment_keys(acc_estimator, target_df):
    target_results = make_result_set(target_df, on_column="product")
    with pytest.raises(ValueError, match="The segmented columns in the reference and target results must be the same."):
        acc_estimator.estimate(target_results)
    target_results = make_result_set(target_df, on_column=["product", "category"])
    with pytest.raises(ValueError, match="The segmented columns in the reference and target results must be the same."):
        acc_estimator.estimate(target_results)


def test_accuracy_estimation(acc_estimator, target_df):
    target_results = make_result_set(target_df)
    estimator_result = acc_estimator.estimate(target_results)
    assert estimator_result.accuracy == 0.625
    assert estimator_result.reference_partition_id is not None
    assert estimator_result.reference_result_timestamp is not None


def test_regression_estimation(acc_estimator, target_df):
    reference_result = make_regression_result_set(target_df)
    acc_estimator = AccuracyEstimator(reference_result)
    target_results = make_result_set(target_df)
    with pytest.raises(ValueError, match="Accuracy estimation currently supported for binary classification problems."):
        acc_estimator.estimate(target_results)


def test_multi_class_estimation():
    # todo: check for multi class once it's supported in whylogs
    pass
