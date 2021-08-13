import pytest

from whylogs.core.metrics.confusion_matrix import ConfusionMatrix
from whylogs.core.metrics.model_metrics import ModelMetrics
from whylogs.core.metrics.regression_metrics import RegressionMetrics
from whylogs.proto import ModelType


def tests_model_metrics():
    mod_met = ModelMetrics(model_type=ModelType.CLASSIFICATION)

    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]

    mod_met.compute_confusion_matrix(predictions_1, targets_1, scores_1)

    assert mod_met.model_type == ModelType.CLASSIFICATION

    for idx, value in enumerate(mod_met.confusion_matrix.labels):
        for jdx, value_2 in enumerate(mod_met.confusion_matrix.labels):
            print(idx, jdx)
            assert mod_met.confusion_matrix.confusion_matrix[idx, jdx].floats.count == expected_1[idx][jdx]


def tests_model_metrics_to_protobuf_classification():
    mod_met = ModelMetrics(model_type=ModelType.CLASSIFICATION)

    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    mod_met.compute_confusion_matrix(predictions_1, targets_1, scores_1)

    message = mod_met.to_protobuf()

    model_metrics = ModelMetrics.from_protobuf(message)
    assert model_metrics.model_type == ModelType.CLASSIFICATION
    assert model_metrics.confusion_matrix.labels == ["cat", "dog", "pig"]


def tests_no_metrics_to_protobuf_classification():
    mod_met = ModelMetrics(model_type=ModelType.CLASSIFICATION)

    assert mod_met.model_type == ModelType.CLASSIFICATION
    message = mod_met.to_protobuf()

    model_metrics = ModelMetrics.from_protobuf(message)
    assert model_metrics.model_type == ModelType.CLASSIFICATION


def tests_no_metrics_to_protobuf_regression():

    mod_met = ModelMetrics(model_type=ModelType.REGRESSION)
    assert mod_met.model_type == ModelType.REGRESSION
    message = mod_met.to_protobuf()

    model_metrics = ModelMetrics.from_protobuf(message)
    assert model_metrics.model_type == ModelType.REGRESSION


def regression_metrics_eq(self, other):
    # test for object attribute equality - only used for tests,
    # defining __eq__ also renders object unhashable.
    # so might be necessary to define __hash__ to behave sanely in dicts and sets,
    if not isinstance(other, RegressionMetrics) or not isinstance(self, RegressionMetrics):
        # don't attempt to compare against unrelated types
        return NotImplemented

    return (
        self.prediction_field == other.prediction_field
        and self.target_field == other.target_field
        and self.count == other.count
        and self.sum_abs_diff == other.sum_abs_diff
        and self.sum_diff == other.sum_diff
        and self.sum2_diff == other.sum2_diff
    )


def tests_model_metrics_to_protobuf_regression():
    regression_model = ModelMetrics(model_type=ModelType.REGRESSION)

    targets_1 = [0.1, 0.3, 0.4]
    predictions_1 = [0.5, 0.5, 0.5]
    regression_model.compute_regression_metrics(predictions=predictions_1, targets=targets_1, target_field="target", prediction_field="prediction")

    regression_message = regression_model.to_protobuf()
    model_metrics_from_message = ModelMetrics.from_protobuf(regression_message)
    assert model_metrics_from_message.model_type == ModelType.REGRESSION
    assert regression_metrics_eq(model_metrics_from_message.regression_metrics, regression_model.regression_metrics)


def test_merge_none():
    metrics = ModelMetrics()
    assert metrics.merge(None) == metrics


def test_merge_metrics_with_none_confusion_matrix():
    metrics = ModelMetrics()
    other = ModelMetrics()
    other.confusion_matrix = None
    metrics.merge(other)


def test_merge_metrics_model():
    metrics = ModelMetrics()
    other = ModelMetrics(model_type=ModelType.REGRESSION)
    other.regression_metrics = None
    new_metrics = metrics.merge(other)
    assert new_metrics.model_type == ModelType.REGRESSION
    assert new_metrics.confusion_matrix is None

    # keep initial model type during merge
    metrics = ModelMetrics(model_type=ModelType.REGRESSION)
    other = ModelMetrics(model_type=ModelType.CLASSIFICATION)
    other.regression_metrics = None
    new_metrics = metrics.merge(other)
    assert new_metrics.model_type == ModelType.REGRESSION
    assert new_metrics.confusion_matrix is None


def test_merge_metrics_with_none_regression_matrix():
    metrics = ModelMetrics()
    other = ModelMetrics(model_type=ModelType.REGRESSION)
    other.regression_metrics = None
    new_metrics = metrics.merge(other)
    assert new_metrics.model_type == ModelType.REGRESSION


def test_merge_metrics_with_none_confusion_matrix():
    metrics = ModelMetrics()
    other = ModelMetrics()
    other.confusion_matrix = None
    other.regression_metrics = None

    new_metrics = metrics.merge(other)
    assert new_metrics.model_type == ModelType.UNKNOWN


def test_model_metrics_init():
    reg_met = RegressionMetrics()
    conf_ma = ConfusionMatrix()
    with pytest.raises(NotImplementedError):
        metrics = ModelMetrics(confusion_matrix=conf_ma, regression_metrics=reg_met)
