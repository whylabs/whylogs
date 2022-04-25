import pandas as pd

from whylogs.core import DatasetProfile, DatasetProfileView


def test_view_serde_roundtriπ() -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile()
    profile.track(pandas=df)
    view = profile.view()
    view.write("/tmp/data.bin")
    res = DatasetProfileView.read("/tmp/data.bin")

    assert len(view.to_pandas()) == len(res.to_pandas())
