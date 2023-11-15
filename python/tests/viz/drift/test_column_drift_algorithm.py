import numpy as np
import pandas as pd
import pytest
from PIL import Image

import whylogs as why
from whylogs.extras.image_metric import log_image
from whylogs.viz.drift.column_drift_algorithms import (
    KS,
    ChiSquare,
    Hellinger,
    calculate_drift_scores,
)
from whylogs.viz.drift.configs import (
    ChiSquareConfig,
    DriftThresholds,
    HellingerConfig,
    KSTestConfig,
)


@pytest.fixture
def target_view():
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [4.3, 1.8, None, 4.1],
    }
    df = pd.DataFrame(data)
    prof_view = why.log(df).profile().view()
    return prof_view


@pytest.fixture
def reference_view():
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [13, 34, 99, 123],
        "weight": [4.9, 13.3, None, 232.3],
    }
    df = pd.DataFrame(data)
    prof_view = why.log(df).profile().view()
    return prof_view


ref_image_data = [np.zeros((100, 100)) for _ in range(20)]
target_image_data = [np.ones((100, 100)) for _ in range(20)]


def create_profile(image_list):
    agg_profile = None
    for img_arr in image_list:
        img_pil = Image.fromarray(img_arr)
        profile = log_image(img_pil).profile()
        profile_view = profile.view()

        if agg_profile is None:
            agg_profile = profile_view
        else:
            agg_profile = agg_profile.merge(profile_view)
    return agg_profile


@pytest.fixture
def target_image_view():
    return create_profile(target_image_data)


@pytest.fixture
def reference_image_view():
    return create_profile(ref_image_data)


def test_calculate_drift_score(target_view, reference_view):
    drift_scores = calculate_drift_scores(target_view=target_view, reference_view=reference_view, with_thresholds=True)
    assert drift_scores["animal"]["algorithm"] == "chi-square"
    assert drift_scores["animal"]["drift_category"] == "NO_DRIFT"
    assert drift_scores["legs"]["algorithm"] == "ks"
    assert drift_scores["legs"]["drift_category"] == "DRIFT"
    assert drift_scores["weight"]["algorithm"] == "ks"
    assert drift_scores["weight"]["drift_category"] == "DRIFT"


def test_calculate_drift_score_with_custom_config(target_view, reference_view):
    hellingerconfig = HellingerConfig(
        max_hist_buckets=50, thresholds=DriftThresholds(NO_DRIFT=(0, 0.1), POSSIBLE_DRIFT=(0.1, 0.5), DRIFT=(0.05, 1))
    )
    chisquareconfig = ChiSquareConfig()
    # binary classification
    ksconfig = KSTestConfig(thresholds=DriftThresholds(NO_DRIFT=(0.15, 1), DRIFT=(0, 0.15)))
    drift_map = {"animal": ChiSquare(chisquareconfig), "weight": Hellinger(hellingerconfig), "legs": KS(ksconfig)}
    drift_scores = calculate_drift_scores(
        target_view=target_view, reference_view=reference_view, drift_map=drift_map, with_thresholds=True
    )
    assert drift_scores["animal"]["algorithm"] == "chi-square"
    assert drift_scores["animal"]["drift_category"] == "NO_DRIFT"
    assert drift_scores["animal"]["thresholds"] == {
        "NO_DRIFT": (0.15, 1),
        "POSSIBLE_DRIFT": (0.05, 0.15),
        "DRIFT": (0, 0.05),
    }
    assert drift_scores["legs"]["algorithm"] == "ks"
    assert drift_scores["legs"]["drift_category"] == "DRIFT"
    assert drift_scores["legs"]["thresholds"] == {"NO_DRIFT": (0.15, 1), "DRIFT": (0, 0.15)}
    assert drift_scores["weight"]["algorithm"] == "hellinger"
    assert drift_scores["weight"]["drift_category"] == "DRIFT"
    assert drift_scores["weight"]["thresholds"] == {
        "NO_DRIFT": (0, 0.1),
        "POSSIBLE_DRIFT": (0.1, 0.5),
        "DRIFT": (0.05, 1),
    }


def test_calculate_drift_wrong_algorithm(target_view, reference_view):
    drift_map = {"animal": Hellinger()}  # wrong algorithm - hellinger needs a distribution metric
    scores = calculate_drift_scores(target_view=target_view, reference_view=reference_view, drift_map=drift_map)
    assert scores["animal"] is None
    assert scores["legs"]  # other columns not in the map are calculated with default behavior
    assert scores["weight"]


def test_calculate_drift_with_nonexisting_column(target_view, reference_view):
    drift_map = {"non-existing": Hellinger()}  # wrong algorithm - hellinger needs a distribution metric
    with pytest.warns(UserWarning, match="Column non-existing not found in target profile."):
        scores = calculate_drift_scores(target_view=target_view, reference_view=reference_view, drift_map=drift_map)
    assert not scores.get("non-existing")
    assert scores["animal"]
    assert scores["legs"]
    assert scores["weight"]


def test_calculate_drift_with_wrong_config(target_view, reference_view):
    with pytest.raises(ValueError, match="Parameter config must be of type HellingerConfig."):
        Hellinger(KSTestConfig())
    with pytest.raises(ValueError, match="Parameter config must be of type KSTestConfig."):
        KS(HellingerConfig())
    with pytest.raises(ValueError, match="Parameter config must be of type ChiSquareConfig."):
        ChiSquare(KSTestConfig())


def test_calculate_drift_conflicting_thresholds(target_view, reference_view):
    hellingerconfig = HellingerConfig(thresholds=DriftThresholds(NO_DRIFT=(0, 0.9), DRIFT=(0.3, 1)))
    drift_map = {"weight": Hellinger(hellingerconfig)}
    scores = calculate_drift_scores(
        target_view=target_view, reference_view=reference_view, drift_map=drift_map, with_thresholds=True
    )
    assert scores["weight"]["drift_category"] == "DRIFT"  # conflicting thresholds, so priority is by drift severity
    hellingerconfig = HellingerConfig(thresholds=DriftThresholds(NO_DRIFT=(0, 0.001), DRIFT=(0.99, 1)))
    with pytest.raises(ValueError, match="does not fit into any drift category defined by thresholds."):
        drift_map = {"weight": Hellinger(hellingerconfig)}
        scores = calculate_drift_scores(
            target_view=target_view, reference_view=reference_view, drift_map=drift_map, with_thresholds=True
        )


def test_calculate_drift_score_with_image(target_image_view, reference_image_view):
    drift_scores = calculate_drift_scores(
        target_view=target_image_view, reference_view=reference_image_view, with_thresholds=True
    )
    assert len(drift_scores) == 11
    assert drift_scores["image.Brightness.mean"]["pvalue"] == 0.0
    assert drift_scores["image.ImagePixelWidth"]["pvalue"] == 1.0
