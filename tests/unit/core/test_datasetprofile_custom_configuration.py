import os

import pytest

from whylogs.v2.core import DatasetProfile
from whylogs.v2.core.model_profile import ModelProfile

TEST_DATA_PATH = os.path.abspath(
    os.path.join(
        os.path.realpath(os.path.dirname(__file__)),
        os.pardir,
        os.pardir,
        os.pardir,
        "testdata",
    )
)


def test_declarative_dataset_profile_metrics_config():
    x1 = DatasetProfile(name="test", model_profile=ModelProfile())
    assert x1.tags["name"] == "test"
    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    x1.track_metrics(predictions_1, targets_1, scores_1)

    assert x1.model_profile.metrics.confusion_matrix.labels is not None
