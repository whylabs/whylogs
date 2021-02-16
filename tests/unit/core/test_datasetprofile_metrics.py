import os

from whylogs.core import DatasetProfile

TEST_DATA_PATH = os.path.abspath(os.path.join(os.path.realpath(
    os.path.dirname(__file__)), os.pardir, os.pardir, os.pardir, "testdata"))


def test_dataset_profile_metrics():
    x1 = DatasetProfile(name="test")
    assert x1.tags["name"] == "test"
    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]

    x1.track_metrics(predictions_1, targets_1, scores_1)

    assert x1.model_profile.metrics.confusion_matrix.labels is not None


def test_java_protobuf():
    file_path = os.path.join(TEST_DATA_PATH, "jvm_output.bin")
    dst_prof = DatasetProfile.read_protobuf(file_path)

    assert dst_prof.name == ""
    assert dst_prof.model_profile is not None
    assert dst_prof.model_profile.metrics is not None
    confusion_M = dst_prof.model_profile.metrics.confusion_matrix
    assert confusion_M is not None
