import math

import pytest
from pyspark.sql import Row, SparkSession
from whyspark import estimate_segments
from whyspark.preprocessing.autosegmentation import _simple_entropy


@pytest.fixture
def spark(scope="session"):
    return SparkSession.builder.master("local").appName("test-autosegmentation").getOrCreate()


@pytest.fixture
def dfA(spark, scope="session"):
    data = [
        Row(weight_lbs=8.6, color="Orange", eye_color="Blue", animal="cat"),
        Row(weight_lbs=31.0, color="Brown", eye_color="Green", animal="dog"),
        Row(weight_lbs=14.8, color="White", eye_color="Black", animal="dog"),
        Row(weight_lbs=15.2, color="White", eye_color="Green", animal="cat"),
        Row(weight_lbs=10.0, color="Black", eye_color="Blue", animal="cat"),
        Row(weight_lbs=9.1, color="Orange", eye_color="Blue", animal="cat"),
    ]
    return spark.createDataFrame(data)


@pytest.fixture
def dfB(spark, scope="session"):
    data = [
        Row(weight_lbs=8.6, lives=9, eye_color="Blue", animal="cat"),
        Row(weight_lbs=31.0, lives=1, eye_color="Green", animal="dog"),
        Row(weight_lbs=14.8, lives=1, eye_color="Black", animal="dog"),
        Row(weight_lbs=15.2, lives=8, eye_color="Null", animal="cat"),
        Row(weight_lbs=10.0, lives=9, eye_color="Null", animal="cat"),
        Row(weight_lbs=9.1, lives=4, eye_color="Blue", animal="cat"),
    ]
    return spark.createDataFrame(data)


def test_estimate_segments_target_field(dfA):
    result = estimate_segments(dfA, target_field="animal")
    assert set(result) == set(["eye_color", "color"])


def test_estimate_segments_target_field_limited(dfA):
    result = estimate_segments(dfA, target_field="animal", max_segments=5)
    assert set(result) == set(["eye_color"])


def test_estimate_segments_no_target_field(dfA):
    result = estimate_segments(dfA)
    assert set(result) == set(["eye_color", "animal"])


def test_estimate_segments_no_target_field_limited(dfA):
    result = estimate_segments(dfA, max_segments=2)
    assert set(result) == set([])


def test_simple_entropy_values(dfA):
    """Compares entropy calculation with scipy calculation, base = 2"""
    assert _simple_entropy(dfA, "animal") == pytest.approx(0.9182958340544896)
    assert _simple_entropy(dfA, "color") == pytest.approx(1.9182958340544893)
    assert _simple_entropy(dfA, "eye_color") == pytest.approx(1.4591479170272448)


def test_estimate_segments_custom_null_bound(dfB):
    result = estimate_segments(dfB, null_perc_bounds=[None, 0.1])
    assert "eye_color" not in result


def test_estimate_segments_custom_unique_bound(dfB):
    result = estimate_segments(dfB, unique_perc_bounds=[0.55, None])
    assert "animal" not in result


def test_estimate_segments_include_columns(dfB):
    result = estimate_segments(dfB, include_columns=["weight_lbs", "lives"], unique_perc_bounds=[None, None])
    assert set(result) == set(["lives", "eye_color"])
