import os.path

import pandas as pd

from whylogs.core import DatasetProfile, DatasetProfileView


def test_view_serde_roundtrip(tmp_path: str) -> None:
    d = {"col1": [1, 2], "col2": [3.0, 4.0], "col3": ["a", "b"]}
    df = pd.DataFrame(data=d)

    profile = DatasetProfile()
    profile.track(pandas=df)
    view = profile.view()
    output_file = os.path.join(tmp_path, "view.bin")
    view.write(output_file)
    res = DatasetProfileView.read(output_file)

    assert len(view.to_pandas()) == len(res.to_pandas())
