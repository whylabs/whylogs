import os

import pandas as pd

import whylogs as why
from whylogs.api.whylabs.log_reference import log_references


def test_log_reference(caplog) -> None:
    os.environ["WHYLABS_HOST"] = "https://songbird.development.whylabsdev.com"
    df = pd.DataFrame({"a": [1.2, 212.0, 3.2, 4.1]})
    df2 = pd.DataFrame({"a": [1.2, 1.232, 311.2, 4.5]})

    why.init(anonymous=True)

    req_results = log_references([df, df2])

    for result in req_results:
        assert result.status_code == 200
        assert "Upload successful! Check your profile at" in caplog.text
