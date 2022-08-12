from dataclasses import dataclass, field
from typing import Any, Dict

import numpy as np
import pandas as pd

from whylogs.core import DatasetSchema
from whylogs.core.resolvers import Resolver, StandardResolver


class MyResolver(StandardResolver):
    pass


@dataclass
class MySchema(DatasetSchema):
    types: Dict[str, Any] = field(
        default_factory=lambda: {
            "col1": str,
            "col2": np.int32,
            "col3": pd.CategoricalDtype(categories=("foo", "bar"), ordered=True),
        }
    )
    resolvers: Resolver = field(default_factory=MyResolver)
    cache_size: int = 12


def test_schema_default_value_overrides() -> None:
    schema = MySchema()
    assert isinstance(schema.resolvers, MyResolver)
    assert schema.types["col2"] == np.int32
    assert schema.cache_size == 12


def test_schema_subclass_copy() -> None:
    schema = MySchema()
    copy = schema.copy()
    assert isinstance(copy, MySchema)
