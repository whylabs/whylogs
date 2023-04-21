import os

import pandas as pd

import whylogs as why
from whylogs.api.whylabs.log_reference import log_references


def test_log_reference() -> None:
    os.environ["WHYLABS_HOST"] = "https://songbird.development.whylabsdev.com"
    df = pd.DataFrame({"a": [1, 2, 3, 4]})
    df2 = df.copy()

    why.init(anonymous=True)

    req_results = log_references([{"data": df}, {"data": df2}])

    for result in req_results:
        assert result.status_code == 200
