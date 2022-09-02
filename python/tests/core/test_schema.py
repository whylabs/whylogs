import numpy as np
import pandas as pd

from whylogs.core import DatasetSchema
from whylogs.core.resolvers import StandardResolver


class MyResolver(StandardResolver):
    pass


def test_schema_default_value_overrides() -> None:
    schema = DatasetSchema(
        types={"col1": str, "col2": np.int32, "col3": pd.CategoricalDtype(categories=("foo", "bar"), ordered=True)},
        resolvers=MyResolver(),
        cache_size=12,
    )
    assert isinstance(schema.resolvers, MyResolver)
    assert schema.types["col2"] == np.int32
    assert schema.cache_size == 12


def test_schema_subclass_copy() -> None:
    schema = DatasetSchema(
        types={"col1": str, "col2": np.int32, "col3": pd.CategoricalDtype(categories=("foo", "bar"), ordered=True)},
        resolvers=MyResolver(),
        cache_size=12,
    )
    copy = schema.copy()
    assert isinstance(copy.resolvers, MyResolver)
    assert copy.types["col2"] == np.int32
    assert copy.cache_size == 12
    assert copy._columns.keys() == schema._columns.keys()
    for col_name, column in schema._columns.items():
        assert copy._columns[col_name].dtype == column.dtype
        assert copy._columns[col_name].resolver.__class__ == column.resolver.__class__
        assert copy._columns[col_name].type_mapper.__class__ == column.type_mapper.__class__
        assert copy._columns[col_name].cfg == column.cfg
