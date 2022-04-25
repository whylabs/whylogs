import logging
from dataclasses import dataclass

logger = logging.getLogger(__name__)

try:
    import pandas as _pd
except ImportError:  # noqa
    _pd = None  # type: ignore

try:
    import numpy as _np
except ImportError:  # noqa
    _np = None  # type: ignore
    if _pd is not None:
        logger.error("Pandas is installed but numpy is not. Your environment is probably broken.")


@dataclass(frozen=True)
class NumpyStub:
    dtype: None = None
    number: None = None
    floating: None = None
    ndarray: None = None
    timedelta64: None = None
    datetime64: None = None
    unicode_: None = None
    issubdtype: None = None


@dataclass(frozen=True)
class PandasStub(object):
    Series: None = None
    DataFrame: None = None


if _np is None:
    _np = NumpyStub()

if _pd is None:
    _pd = PandasStub()

np = _np
pd = _pd
