from logging import getLogger

from whylogs.core.model_performance_metrics import ModelPerformanceMetrics
from whylogs.core.model_performance_metrics.confusion_matrix import ConfusionMatrix
from whylogs.core.proto.v0 import ModelProfileMessage

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
    mod_prof.compute_regression_metrics(
        predictions=predictions, targets=targets, target_field="col3", prediction_field="output.col3"
    )

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
