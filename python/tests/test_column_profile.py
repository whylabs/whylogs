import unittest

import numpy as np
import pandas as pd

from whylogs_v1.core import ColumnProfile, ColumnProfileView, ColumnSchema


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

        msg = col_prof.serialize()
        view = ColumnProfileView.deserialize(msg)
        assert view.get_metric("hist") is not None

        # must have histogram
        assert col_prof._trackers["hist"] is not None

    def test_basic_str_column(self) -> None:
        series = pd.Series(["a", "b", "c"])
        schema = ColumnSchema(series.dtype)
        col_prof = ColumnProfile("string", schema, cache_size=1024)
        col_prof.track_column(series)
        col_prof.flush()

        msg = col_prof.serialize()
        view = ColumnProfileView.deserialize(msg)
        assert view.get_metric("hist") is None

        # histogram should be None
        assert col_prof._trackers.get("hist") is None
        assert col_prof._trackers.get("fi") is not None


if __name__ == "__main__":
    unittest.main()
