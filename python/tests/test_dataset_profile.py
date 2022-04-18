import numpy
import pandas as pd

from whylogs.core import DatasetProfile, DatasetProfileView, DatasetSchema


def test_basic_dataset_profile() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile()
    profile.track(pandas=df)
    profile.serialize()

    assert profile._columns["col1"]._schema.dtype == numpy.int64
    assert profile._columns["col2"]._schema.dtype == numpy.float64
    assert profile._columns["col3"]._schema.dtype.name == "object"


def test_override_schema_col2_as_string() -> None:
    d = {"col1": [1, 2, 3], "col2": [3.0, 4.0, "c"], "col3": ["a", "b", "c"]}
    df = pd.DataFrame(data=d)

    class MyCustomSchema(DatasetSchema):
        types = {
            "col2": str,
        }

    profile = DatasetProfile(MyCustomSchema())
    profile.track(pandas=df)
    msg = profile.serialize()
    view = DatasetProfileView.deserialize(msg)
    assert view.get_column("col1") is not None
    view2 = profile.view()
    assert view2.get_column("col1") is not None
    view.merge(view).merge(view)

    assert profile._columns["col1"]._schema.dtype == numpy.int64
    assert profile._columns["col2"]._schema.dtype == str
    assert profile._columns["col3"]._schema.dtype.name == "object"
    pdf = view.to_pandas()
    print(pdf)


def test_basic_iter_row() -> None:
    d = {"col1": [1, 2, 3], "col2": [3.0, 4.0, "c"], "col3": ["a", "b", "c"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile()
    for row in df.iterrows():
        profile.track(row=row[1].to_dict())  # type: ignore

    assert profile._columns["col1"]._schema.dtype == int
    assert profile._columns["col2"]._schema.dtype == float
    assert profile._columns["col3"]._schema.dtype == str
