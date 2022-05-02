import unittest

import numpy as np
import pandas as pd

from whylogs.core import ColumnProfile, ColumnProfileView, ColumnSchema


class TestListElements(unittest.TestCase):
    def test_basic_int_column(self) -> None:
        series = pd.Series(list(range(0, 10)))
        schema = ColumnSchema(dtype=series.dtype)
        col_prof = ColumnProfile("numbers", schema, cache_size=1024)
        col_prof.track_column(series)
        col_prof.track_column(np.array(range(0, 100), float))
        col_prof.track_column(np.array(range(0, 100), int))
        col_prof.track_column(["a", "b", "c"])
        col_prof.flush()

        # must have dist and types
        assert col_prof._metrics["dist"] is not None
        assert col_prof._metrics["types"] is not None

        msg = col_prof.to_protobuf()
        assert msg.metric_components["fi/fs"] is not None
        assert msg.metric_components["cnt/n"] is not None
        assert msg.metric_components["cnt/null"] is not None
        assert msg.metric_components["dist/kll"] is not None
        assert msg.metric_components["dist/mean"] is not None
        assert msg.metric_components["dist/m2"] is not None

        view = ColumnProfileView.from_protobuf(msg)
        assert view.get_metric("dist") is not None

    def test_basic_str_column(self) -> None:
        series = pd.Series(["a", "b", "c"])
        schema = ColumnSchema(series.dtype)
        col_prof = ColumnProfile("string", schema, cache_size=1024)
        col_prof.track_column(series)
        col_prof.flush()

        msg = col_prof.to_protobuf()
        view = ColumnProfileView.from_protobuf(msg)
        assert view.get_metric("dist") is None

        # histogram should be None
        assert col_prof._metrics.get("dist") is None
        assert col_prof._metrics.get("fi") is not None


if __name__ == "__main__":
    unittest.main()
