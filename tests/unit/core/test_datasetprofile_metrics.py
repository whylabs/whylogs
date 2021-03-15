import os
import pytest

from whylogs.core import DatasetProfile
from whylogs.core.model_profile import ModelProfile

TEST_DATA_PATH = os.path.abspath(os.path.join(os.path.realpath(
    os.path.dirname(__file__)), os.pardir, os.pardir, os.pardir, "testdata"))


def test_dataset_profile_metrics():
    x1 = DatasetProfile(name="test", model_profile=ModelProfile())
    assert x1.tags["name"] == "test"
    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]

    x1.track_metrics(predictions_1, targets_1, scores_1)

    assert x1.model_profile.metrics.confusion_matrix.labels is not None


def test_read_java_protobuf():
    file_path = os.path.join(TEST_DATA_PATH, "jvm_output.bin")
    dst_prof = DatasetProfile.read_protobuf(file_path)
    labels = ["0", "1"]
    assert dst_prof.tags["Name"] == "my-model-name"
    assert dst_prof.name == "my-model-name"
    assert dst_prof.model_profile is not None
    assert dst_prof.model_profile.metrics is not None
    confusion_M = dst_prof.model_profile.metrics.confusion_matrix
    assert confusion_M.labels is not None
    assert len(confusion_M.labels) == 2
    for idx, lbl in enumerate(confusion_M.labels):
        assert lbl == labels[idx]



def test_parse_from_protobuf_with_regression():
    dir_path = os.path.dirname(os.path.realpath(__file__))
    prof= DatasetProfile.read_protobuf(os.path.join(
        TEST_DATA_PATH, "metrics","regression_java.bin"))
    assert prof.name == 'my-model-name'
    assert prof.model_profile is not None
    assert prof.model_profile.metrics is not None
    confusion_M = prof.model_profile.metrics.confusion_matrix
    regression_met= prof.model_profile.metrics.regression_metrics
    assert regression_met is not None
    assert confusion_M is None
    # metrics
    assert regression_met.count==89
    assert regression_met.sum_abs_diff==pytest.approx(7649.1, 0.1)
    assert regression_met.sum_diff==pytest.approx(522.7, 0.1)
    assert regression_met.sum2_diff==pytest.approx(1021265.7, 0.1)
