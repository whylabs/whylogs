import pytest
from whylogs.drift.configs import KSTestConfig, HellingerConfig, ChiSquareConfig, DriftThresholds
from whylogs.drift.column_drift_algorithms import calculate_drift_scores
from whylogs.drift.column_drift_algorithms import KS, Hellinger, ChiSquare
import pandas as pd
import whylogs as why


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
    ksconfig = KSTestConfig(thresholds=DriftThresholds(NO_DRIFT=(0.15, 1.1), DRIFT=(0, 0.15)))
    drift_map = {"animal": ChiSquare(chisquareconfig), "weight": Hellinger(hellingerconfig), "legs": KS(ksconfig)}
    drift_scores = calculate_drift_scores(
        target_view=target_view, reference_view=reference_view, drift_map=drift_map, with_thresholds=True
    )
    assert drift_scores["animal"]["algorithm"] == "chi-square"
    assert drift_scores["animal"]["drift_category"] == "NO_DRIFT"
    assert drift_scores["animal"]["thresholds"] == {
        "NO_DRIFT": (0.15, 1.1),
        "POSSIBLE_DRIFT": (0.05, 0.15),
        "DRIFT": (0, 0.05),
    }
    assert drift_scores["legs"]["algorithm"] == "ks"
    assert drift_scores["legs"]["drift_category"] == "DRIFT"
    assert drift_scores["legs"]["thresholds"] == {"NO_DRIFT": (0.15, 1.1), "DRIFT": (0, 0.15)}
    assert drift_scores["weight"]["algorithm"] == "hellinger"
    assert drift_scores["weight"]["drift_category"] == "POSSIBLE_DRIFT"
    assert drift_scores["weight"]["thresholds"] == {
        "NO_DRIFT": (0, 0.1),
        "POSSIBLE_DRIFT": (0.1, 0.5),
        "DRIFT": (0.05, 1),
    }
