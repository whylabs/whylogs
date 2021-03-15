import os

import pandas as pd
import pytest

from whylogs.core.metrics.regression_metrics import RegressionMetrics
from whylogs.proto import RegressionMetricsMessage



TEST_DATA_PATH = os.path.abspath(os.path.join(os.path.realpath(
    os.path.dirname(__file__)), os.pardir,os.pardir, os.pardir, os.pardir, "testdata"))

def my_test():
    regmet= RegressionMetrics()
    assert regmet.count == 0
    assert regmet.sum_diff==0.0
    assert regmet.sum2_diff ==0.0
    assert regmet.sum_abs_diff==0.0


def test_load_parquet():
    mean_absolute_error=85.94534216005789
    mean_squared_error =11474.89611670205
    root_mean_squared_error =107.12094154133472

    regmet=RegressionMetrics()
    df= pd.read_parquet(os.path.join(os.path.join(TEST_DATA_PATH,"metrics","2021-02-12.parquet")))
    regmet.add(df["predictions"].to_list(),df["targets"].to_list())

    assert regmet.count==len(df["predictions"].to_list())
    assert regmet.mean_squared_error()==pytest.approx(mean_squared_error,0.01)

    assert regmet.mean_absolute_error() == pytest.approx(mean_absolute_error,0.01)
    assert regmet.root_mean_squared_error() == pytest.approx(root_mean_squared_error,0.01)

    msg= regmet.to_protobuf()
    new_regmet= RegressionMetrics.from_protobuf(msg)
    assert regmet.count  ==new_regmet.count
    assert regmet.mean_squared_error()  ==new_regmet.mean_squared_error() 
    assert regmet.root_mean_squared_error()  ==new_regmet.root_mean_squared_error() 
    assert regmet.mean_absolute_error()  ==new_regmet.mean_absolute_error() 

def test_empty_protobuf_should_return_none():
    empty_message = RegressionMetricsMessage()
    assert RegressionMetrics.from_protobuf(empty_message) is None



