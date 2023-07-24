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

try:
    import scipy as _sp
except ImportError:  # noqa
    _sp = None  # type: ignore

try:
    import sklearn.cluster as _sklc
    import sklearn.decomposition as _skld
    import sklearn.metrics.pairwise as _sklp
except ImportError:  # noqa
    _sklp = None  # type: ignore
    _sklc = None  # type: ignore
    _skld = None  # type: ignore


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
    integer: type = _StubClass


@dataclass(frozen=True)
class PandasStub(object):
    Series: type = _StubClass
    DataFrame: type = _StubClass


@dataclass(frozen=True)
class ScipyStub:
    sparse: type = _StubClass


@dataclass(frozen=True)
class ScikitLearnStub:
    cosine_distances: type = _StubClass
    euclidean_distances: type = _StubClass
    KMeans: type = _StubClass
    PCA: type = _StubClass


def is_not_stub(stubbed_class: Any) -> bool:
    if (
        stubbed_class
        and stubbed_class is not _StubClass
        and not isinstance(stubbed_class, (PandasStub, NumpyStub, ScipyStub, ScikitLearnStub))
    ):
        return True
    return False


if _np is None:
    _np = NumpyStub()

if _pd is None:
    _pd = PandasStub()

if _sp is None:
    _sp = ScipyStub()

if _sklp is None:
    _sklp = ScikitLearnStub()

if _sklc is None:
    _sklc = ScikitLearnStub()

if _skld is None:
    _skld = ScikitLearnStub()


np = _np
pd = _pd
sp = _sp
sklp = _sklp
sklc = _sklc
skld = _skld
