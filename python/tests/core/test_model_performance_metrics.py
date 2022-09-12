from whylogs.core.model_performance_metrics import ModelPerformanceMetrics
from whylogs.core.proto.v0 import ModelProfileMessage


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
