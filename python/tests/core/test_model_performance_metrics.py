from logging import getLogger

import pandas as pd
import pytest
import base64

from whylogs import log_classification_metrics, log_regression_metrics
from whylogs.core.model_performance_metrics import ModelPerformanceMetrics
from whylogs.core.model_performance_metrics.confusion_matrix import ConfusionMatrix
from whylogs.core.proto.v0 import ModelProfileMessage
from whylogs.core.schema import DatasetSchema
from whylogs.core.segment import Segment
from whylogs.core.segmentation_partition import segment_on_column
from whylogs.core.proto.v0 import ScoreMatrixMessage

from whylogs.core.model_performance_metrics.confusion_matrix import ConfusionMatrix

TEST_LOGGER = getLogger(__name__)


def test_model_performance_metrics_basic():
    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.4, 0.4, 0.2]

    mod_prof = ModelPerformanceMetrics()
    assert scores_1 is not None
    mod_prof.compute_confusion_matrix(targets=targets_1, predictions=predictions_1, scores=scores_1)
    assert mod_prof.output_fields is None
    assert mod_prof.regression_metrics is None
    assert mod_prof.confusion_matrix is not None
    message = mod_prof.to_protobuf()
    deserialized_model_perf = ModelPerformanceMetrics.from_protobuf(message)
    assert deserialized_model_perf is not None
    assert deserialized_model_perf.confusion_matrix is not None
    assert deserialized_model_perf.confusion_matrix.labels is not None
    assert deserialized_model_perf.confusion_matrix.labels == mod_prof.confusion_matrix.labels


def test_roundtrip_serialization():
    original = ModelPerformanceMetrics(field_metadata={"test": set(["output"])})
    serialized_bytes = original.to_protobuf().SerializeToString()
    roundtrip: ModelPerformanceMetrics = ModelPerformanceMetrics.from_protobuf(
        ModelProfileMessage.FromString(serialized_bytes)
    )
    roundtrip.to_protobuf()
    assert roundtrip.output_fields == ["test"]
    assert isinstance(roundtrip.output_fields, list)


def test_binary_classification_should_be_correct():
    confusion_matrix = ConfusionMatrix(labels=[0, 1])
    predictions = [0, 1, 1, 0, 0, 1, 1]
    targets = [1, 0, 1, 1, 0, 1, 1]
    confusion_matrix.add(predictions=predictions, targets=targets, scores=None)

    matrix = confusion_matrix.confusion_matrix
    assert len(matrix) == 4

    # Result matrix
    # [1, 2]
    # [1, 3]
    assert matrix[(0, 0)].n == 1
    assert matrix[(0, 1)].n == 2
    assert matrix[(1, 0)].n == 1
    assert matrix[(1, 1)].n == 3


def test_binary_classification_compute_confusion_matrix():
    import cProfile
    import pstats
    from io import StringIO
    from random import randint

    number_of_rows = 1000
    number_of_classes = 2
    max_label = number_of_classes - 1
    confusion_matrix = ConfusionMatrix(labels=[i for i in range(number_of_classes)])
    predictions = [randint(0, max_label) for _ in range(number_of_rows)]
    targets = [randint(0, max_label) for _ in range(number_of_rows)]

    profiler = cProfile.Profile()
    string_output_stream = StringIO()
    profiler.enable()
    confusion_matrix.add(predictions=predictions, targets=targets, scores=None)
    profiler.disable()
    stats = pstats.Stats(profiler, stream=string_output_stream).sort_stats("cumulative")
    stats.print_stats(10)
    TEST_LOGGER.info(
        f"Computed confusion matrix with {number_of_classes} labels and {number_of_rows} rows, stats "
        f"are\n{string_output_stream.getvalue()}"
    )

    TEST_LOGGER.info(confusion_matrix.confusion_matrix)
    assert confusion_matrix.confusion_matrix is not None


def test_model_performance_metrics_binary():
    predictions = [0, 1, 1, 0, 0, 1, 1]
    targets = [1, 0, 1, 1, 0, 1, 1]

    mod_prof = ModelPerformanceMetrics()
    mod_prof.compute_confusion_matrix(targets=targets, predictions=predictions)

    assert mod_prof.regression_metrics is None
    assert mod_prof.confusion_matrix is not None
    matrix = mod_prof.confusion_matrix.confusion_matrix
    message = mod_prof.to_protobuf()
    deserialized_model_perf = ModelPerformanceMetrics.from_protobuf(message)
    assert deserialized_model_perf is not None
    assert deserialized_model_perf.confusion_matrix is not None
    assert deserialized_model_perf.confusion_matrix.labels is not None

    deserialized_matrix = deserialized_model_perf.confusion_matrix.confusion_matrix
    for i in range(2):
        for j in range(2):
            assert matrix[(i, j)].n == deserialized_matrix[(i, j)].n


def test_model_performance_metrics_regression():
    predictions = [21.6, 201.0, 37.0, 5.34]
    targets = [20, 200, 56.3, 1]

    mod_prof = ModelPerformanceMetrics()
    mod_prof.compute_regression_metrics(predictions=predictions, targets=targets)

    assert mod_prof.regression_metrics is not None
    assert mod_prof.confusion_matrix is None

    message = mod_prof.to_protobuf()
    deserialized_model_perf = ModelPerformanceMetrics.from_protobuf(message)
    assert deserialized_model_perf is not None
    assert (
        deserialized_model_perf.regression_metrics.mean_absolute_error()
        == mod_prof.regression_metrics.mean_absolute_error()
    )
    TEST_LOGGER.info(f"regression metrics are: {mod_prof.regression_metrics.to_protobuf()}")


def test_output_field_set():
    output_fields = ["test", "output.test"]
    original = ModelPerformanceMetrics()
    original.specify_output_fields(set(output_fields))
    serialized_bytes = original.to_protobuf().SerializeToString()
    roundtrip: ModelPerformanceMetrics = ModelPerformanceMetrics.from_protobuf(
        ModelProfileMessage.FromString(serialized_bytes)
    )
    roundtrip.to_protobuf()
    assert sorted(roundtrip.output_fields) == sorted(output_fields)
    assert isinstance(roundtrip.output_fields, list)


@pytest.mark.parametrize("profile_inputs", [True, False])
def test_profile_write_top_level_api_classification(profile_inputs: bool):
    input_rows = 20
    prediction_column = "col3"
    target_column = "col3.ground_truth"
    number_of_classes = 2
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        prediction_column: [f"x{str(i%number_of_classes)}" for i in range(input_rows)],
        target_column: [f"x{str((i+1)%number_of_classes)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    results = log_classification_metrics(
        df, target_column=target_column, prediction_column=prediction_column, log_full_data=profile_inputs
    )

    metrics: ModelPerformanceMetrics = results.performance_metrics
    assert metrics is not None
    assert metrics.confusion_matrix is not None
    if profile_inputs:
        assert results.view().get_column("col1") is not None
    else:
        assert results.view().get_column("col1") is None


@pytest.mark.parametrize("profile_inputs", [True, False])
def test_profile_write_top_level_api_regression(profile_inputs):
    input_rows = 20
    prediction_column = "col1"
    target_column = "col1.ground_truth"

    d = {
        target_column: [i for i in range(input_rows)],
        prediction_column: [i * i * 1.1 for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)

    results = log_regression_metrics(
        df, target_column=target_column, prediction_column=prediction_column, log_full_data=profile_inputs
    )

    metrics: ModelPerformanceMetrics = results.performance_metrics
    assert metrics is not None
    assert metrics.regression_metrics is not None
    if profile_inputs:
        assert results.view().get_column(prediction_column) is not None
    else:
        assert results.view().get_column(prediction_column) is None


def test_profile_write_top_level_api_back_compat():
    input_rows = 2
    prediction_column = "col3"
    target_column = "col3.ground_truth"
    number_of_classes = 2
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        prediction_column: [f"x{str(i%number_of_classes)}" for i in range(input_rows)],
        target_column: [f"x{str((i+1)%number_of_classes)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    results1 = log_classification_metrics(df, target_column=target_column, prediction_column=prediction_column)
    results2 = log_regression_metrics(df, target_column="col1", prediction_column="col2")

    metrics1: ModelPerformanceMetrics = results1.performance_metrics
    assert metrics1 is not None
    assert metrics1.confusion_matrix is not None
    metrics2: ModelPerformanceMetrics = results2.performance_metrics
    assert metrics2 is not None
    assert metrics2.regression_metrics is not None
    assert results1.view().get_column(prediction_column) is None
    assert results2.view().get_column(prediction_column) is None


def test_profile_top_level_api_segmented_performance():
    input_rows = 10
    prediction_column = "col3"
    target_column = "col3.ground_truth"
    segment_column = "seg_col"
    number_of_segments = 5
    number_of_classes = 2
    d = {
        "col1": [i for i in range(input_rows)],
        "col2": [i * i * 1.1 for i in range(input_rows)],
        segment_column: ["seg_" + str(i % number_of_segments) for i in range(input_rows)],
        prediction_column: [f"x{str(i%number_of_classes)}" for i in range(input_rows)],
        target_column: [f"x{str((i%5)%number_of_classes)}" for i in range(input_rows)],
    }

    df = pd.DataFrame(data=d)
    segmented_schema = DatasetSchema(segments=segment_on_column(segment_column))
    segmented_classification_results = log_classification_metrics(
        df,
        target_column=target_column,
        prediction_column=prediction_column,
        schema=segmented_schema,
        log_full_data=True,
    )
    segmented_regression_results = log_regression_metrics(
        df, target_column="col1", prediction_column="col2", schema=segmented_schema
    )

    assert segmented_classification_results.count == number_of_segments
    assert segmented_regression_results.count == number_of_segments
    partitions = segmented_classification_results.partitions
    assert len(partitions) == 1
    partition = partitions[0]
    segments = segmented_classification_results.segments_in_partition(partition)
    assert len(segments) == number_of_segments

    first_segment: Segment = next(iter(segments))
    first_segment_profile = segmented_classification_results.profile(first_segment)

    # assert first_segment.key == ("0",)
    assert first_segment_profile is not None

    metrics1: ModelPerformanceMetrics = first_segment_profile.model_performance_metrics
    assert metrics1 is not None
    assert metrics1.confusion_matrix is not None
    assert metrics1.confusion_matrix.labels == ["x0", "x1"]
    assert metrics1.confusion_matrix.confusion_matrix.get((0, 0)).n == 1
    assert metrics1.confusion_matrix.confusion_matrix.get((1, 0)).n == 1

def test_deserialize_confusion_matrix_with_kll_floats() -> None:
    sample = \
        'CgEwCgExEhxkZWxpdmVyeV9wcmVkaWN0aW9uIChvdXRwdXQpGhhkZWxpdmVyeV9zdGF0dXMgKG91dHB1dCkiHGRlbGl2ZXJ5X2NvbmZpZGVuY2UgKG91dHB1dClSFgoAIggCAQ8BAAEIADIIAQMDAAAezJNSFgoAIggCAQ8BAAEIADIIAQMDAAAezJNS0QEKFAgKEUcu/yH99rU/GeF6FK5H4eo/Eh0IChGuR+F6FK7nPxkAAAAAAADwPyHNzMzMzMwgQCJIBQEPAAABCAAKAAAAAAAAAAABAQD2AAAApHA9PwAAgD/Xo3A/uB5FP6RwPT/NzEw/PQpXP0jhej+kcD0/cT1KPwAAgD/NzEw/MlACAwMAABrMkwgAAAAAAAAA08wssPU9DQg/rZkmPpX4CJp1jlD5me4aoWIjAG6MNiCLJTyeLc5zUuyIHr4Td6ZphdLO5crXo3zWNEUNX4OGflIWCgAiCAIBDwEAAQgAMggBAwMAAB7Mkw=='
    bytes = base64.b64decode(sample)
    msg = ScoreMatrixMessage()
    msg.ParseFromString(bytes)
    confusion_matrix = ConfusionMatrix.from_protobuf(msg)
    assert confusion_matrix is not None
    merged = confusion_matrix.merge(confusion_matrix)
    merged.to_protobuf()
