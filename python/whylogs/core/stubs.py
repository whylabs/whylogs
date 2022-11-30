import logging
from dataclasses import dataclass
from typing import Any

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


class _StubClass:
    pass


@dataclass(frozen=True)
class NumpyStub:
    dtype: type = _StubClass
    number: type = _StubClass
    bool_: type = _StubClass
    floating: type = _StubClass
    ndarray: type = _StubClass
    timedelta64: type = _StubClass
    datetime64: type = _StubClass
    unicode_: type = _StubClass
    issubdtype: type = _StubClass


@dataclass(frozen=True)
class PandasStub(object):
    Series: type = _StubClass
    DataFrame: type = _StubClass


def is_not_stub(stubbed_class: Any) -> bool:
    if stubbed_class and stubbed_class is not _StubClass and not isinstance(stubbed_class, (PandasStub, NumpyStub)):
        return True
    return False


if _np is None:
    _np = NumpyStub()

if _pd is None:
    _pd = PandasStub()

np = _np
pd = _pd
