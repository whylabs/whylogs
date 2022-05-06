import unittest

import numpy as np
import pandas as pd

from whylogs.core import ColumnProfile, ColumnProfileView, ColumnSchema


class TestColumnProfile(unittest.TestCase):
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
        assert col_prof._metrics["distribution"] is not None
        assert col_prof._metrics["types"] is not None

        msg = col_prof.to_protobuf()
        all_keys = set(msg.metric_components.keys())
        assert "frequent_items/frequent_strings" in all_keys
        assert "counts/n" in all_keys
        assert "counts/null" in all_keys
        assert "distribution/kll" in all_keys
        assert "distribution/mean" in all_keys
        assert "distribution/m2" in all_keys
        assert "types/integral" in all_keys
        assert "types/boolean" in all_keys
        assert "types/object" in all_keys
        assert "types/string" in all_keys
        assert "types/fractional" in all_keys
        assert "ints/min" in all_keys
        assert "ints/max" in all_keys
        assert "cardinality/hll" in all_keys

        view = ColumnProfileView.from_protobuf(msg)
        assert view.get_metric("distribution") is not None

    def test_basic_str_column(self) -> None:
        series = pd.Series(["a", "b", "c"])
        schema = ColumnSchema(series.dtype)
        col_prof = ColumnProfile("string", schema, cache_size=1024)
        col_prof.track_column(series)
        col_prof.flush()

        msg = col_prof.to_protobuf()
        view = ColumnProfileView.from_protobuf(msg)
        assert view.get_metric("distribution") is None

        # histogram should be None
        assert col_prof._metrics.get("distribution") is None
        assert col_prof._metrics.get("frequent_items") is not None


if __name__ == "__main__":
    unittest.main()
