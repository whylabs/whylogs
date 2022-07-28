import numpy as np
import pandas as pd
import pytest

import whylogs as why
from whylogs.core.constraints import ConstraintsBuilder


@pytest.fixture
def builder(profile_view):
    builder = ConstraintsBuilder(dataset_profile_view=profile_view)
    return builder


@pytest.fixture
def nan_builder():
    df = pd.DataFrame([np.nan, np.nan, np.nan], columns=["a"])
    profile_view = why.log(pandas=df).view()
    nan_builder = ConstraintsBuilder(dataset_profile_view=profile_view)
    return nan_builder
