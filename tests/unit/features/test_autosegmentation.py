import pandas as pd
import pytest

from whylogs.features.autosegmentation import _entropy, _weighted_entropy,_estimate_segments

# import numpy as np


def test_entropy_ints():
    series = pd.Series([0, 1, 2, 4])
    res = _entropy(series, False)

    assert pytest.approx(res, 0.001) == 1.386
    res_norm = _entropy(series, True)
    assert pytest.approx(res_norm, 0.01) == 1


def test_entropy_string():
    series = pd.Series(["hat", "jug", "hat"])
    res = _entropy(series, False)
    # assert type(res) == np.float64
    assert pytest.approx(res, 0.001) == 0.636
    res_norm = _entropy(series, True)
    assert pytest.approx(res_norm, 0.01) == 0.918


def test_weighted_entropy():

    df = pd.DataFrame({"targets": ["hat", "jug", "hat"], "confidence": [1.2, 3.4, 4.5], "sentiment": ["happy", "sad", "sad"]})
    res = _weighted_entropy(df, [], "confidence")
    assert pytest.approx(res, 0.001) == 0.999
    res_norm = _weighted_entropy(df, [], "confidence", normalized=False)
    assert pytest.approx(res_norm, 0.001) == 1.098
    res = _weighted_entropy(df, ["targets"], "confidence")
    assert pytest.approx(res, 0.001) == 0.666
    res_norm = _weighted_entropy(df, ["targets"], "confidence", normalized=False)
    assert pytest.approx(res_norm, 0.001) == 0.462

def test_estimate_segments():

    df = pd.DataFrame({"target": ["hat", "jug", "hat"], "confidence": [1.2, 3.4, 4.5], "sentiment": ["happy", "sad", "sad"]})
    res = _estimate_segments(df,target_field="confidence",max_segments=4)
    assert res ==  ["target"]

    res = _estimate_segments(df,target_field="confidence",max_segments=3)
    assert res ==  ["target"]

    res = _estimate_segments(df,target_field="confidence",max_segments=1)
    assert res ==  []

    
