import pytest

from whylogs.proto import ModelType
from whylogs.core.metrics.model_metrics import ModelMetrics
from whylogs.core.metrics.confusion_matrix import ConfusionMatrix
from whylogs.core.metrics.regression_metrics import RegressionMetrics


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
            assert mod_met.confusion_matrix.confusion_matrix[idx,
                                                             jdx].floats.count == expected_1[idx][jdx]


def tests_model_metrics_to_protobuf():
    mod_met = ModelMetrics(model_type=ModelType.CLASSIFICATION)

    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]

    mod_met.compute_confusion_matrix(predictions_1, targets_1, scores_1)

    message = mod_met.to_protobuf()

    ModelMetrics.from_protobuf(message)


def test_merge_none():
    metrics = ModelMetrics()
    metrics.merge(None)


def test_merge_metrics_with_none_confusion_matrix():
    metrics = ModelMetrics()
    other = ModelMetrics()
    other.confusion_matrix = None
    new_metrics = metrics.merge(other)


def test_merge_metrics_with_none_regression_matrix():
    metrics = ModelMetrics()
    other = ModelMetrics()
    other.regression_metrics = None
    new_metrics= metrics.merge(other)

def test_merge_metrics_with_none_confusion_matrix():
    metrics = ModelMetrics()
    other = ModelMetrics()
    other.confusion_matrix = None
    other.regression_metrics = None

    new_metrics = metrics.merge(other)

def test_model_metrics_init():
    reg_met = RegressionMetrics()
    conf_ma= ConfusionMatrix()
    with pytest.raises(NotImplementedError):
        metrics = ModelMetrics(confusion_matrix=conf_ma, regression_metrics=reg_met)
    
