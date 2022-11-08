import itertools
import logging
from dataclasses import dataclass
from typing import Any, Iterable, Iterator, List, Optional

from whylogs.core.stubs import is_not_stub, np, pd

logger = logging.getLogger("whylogs.core.views")

try:
    import pandas.core.dtypes.common as pdc
except:  # noqa
    pass


@dataclass
class ListView:
    ints: Optional[List[int]] = None
    floats: Optional[List[float]] = None
    strings: Optional[List[str]] = None
    objs: Optional[List[Any]] = None

    def iterables(self) -> List[List[Any]]:
        it_list = []
        for lst in [self.ints, self.floats, self.strings, self.objs]:
            if lst is not None and len(lst) > 0:
                it_list.append(lst)
        return it_list


@dataclass
class NumpyView:
    ints: Optional[np.ndarray] = None
    floats: Optional[np.ndarray] = None

    def iterables(self) -> List[np.ndarray]:
        it_list = []
        for lst in [self.ints, self.floats]:
            if lst is not None and len(lst) > 0:
                it_list.append(lst)
        return it_list

    @property
    def len(self) -> int:
        length = 0
        if self.ints is not None:
            length += len(self.ints)
        if self.floats is not None:
            length += len(self.floats)

        return length


@dataclass
class PandasView:
    strings: Optional[pd.Series] = None
    objs: Optional[pd.Series] = None

    def iterables(self) -> List[pd.Series]:
        it_list = []
        for lst in [self.strings, self.objs]:
            if lst is not None and len(lst) > 0:
                it_list.append(lst)
        return it_list


@dataclass(init=False)
class PreprocessedColumn:
    """View of a column with data of various underlying storage.

    If Pandas is available, we will use Pandas to handle batch processing.
    If numpy is available, we will use ndarray for numerical values.
    Otherwise, we preprocess values into typed lists for downstream consumers.
    We also track the null count and ensure that processed lists/Series don't contain null values.
    """

    numpy: NumpyView
    pandas: PandasView
    list: ListView
    # TODO: other data sources such as Apache Arrow here

    null_count: int = 0
    len: int = 0
    original: Any = None

    def __init__(self) -> None:
        self.numpy = NumpyView()
        self.pandas = PandasView()
        self.list = ListView()
        self.null_count = 0
        self.nan_count = 0
        self.inf_count = 0
        self.bool_count = 0
        self.bool_count_where_true = 0
        self.len = -1

    def _pandas_split(self, series: pd.Series, parse_numeric_string: bool = False) -> None:
        """
        Split a Pandas Series into numpy array and other Pandas series.

        Args:
            series: the original Pandas series
            parse_numeric_string: if set, this will coerce values into integer using pands.to_numeric() method.

        Returns:
            SplitSeries with multiple values, including numpy arrays for numbers, and strings as a Pandas Series.
        """
        if series is None:
            return None
        if pd.Series is None:
            return None

        null_series = series[series.isnull()]
        non_null_series = series[series.notnull()]

        self.null_count = len(null_series)
        if pdc.is_numeric_dtype(series.dtype) and not pdc.is_bool_dtype(series.dtype):
            if series.hasnans:
                nan_mask = null_series.apply(lambda x: pdc.is_number(x))
                self.nan_count = len(null_series[nan_mask])

            if pdc.is_float_dtype(series.dtype):
                floats = non_null_series.astype(float)
                inf_mask = floats.apply(lambda x: np.isinf(x))
                self.inf_count = len(floats[inf_mask])
                self.numpy.floats = floats
                return
            else:
                ints = non_null_series.astype(int)
                self.numpy.ints = ints
                return

        if series.hasnans:
            na_number_mask = null_series.apply(lambda x: pdc.is_number(x))
            if not null_series[na_number_mask].empty:
                nan_floats = null_series[na_number_mask].astype(float)
                self.nan_count = nan_floats.isna().sum()

        # if non_null_series is empty, then early exit.
        # this fixes a bug where empty columns produce masks of types other than bool
        # and DatetimeArrays do not support | operator for example.
        if non_null_series.empty:
            return

        if issubclass(series.dtype.type, str):
            self.pandas.strings = non_null_series
            return

        if parse_numeric_string:
            non_null_series = pd.to_numeric(non_null_series, errors="ignore")

        float_mask = non_null_series.apply(lambda x: pdc.is_float(x) or pdc.is_decimal(x))
        bool_mask = non_null_series.apply(lambda x: pdc.is_bool(x))
        bool_mask_where_true = non_null_series.apply(lambda x: pdc.is_bool(x) and x)
        int_mask = non_null_series.apply(lambda x: pdc.is_number(x) and pdc.is_integer(x) and not pdc.is_bool(x))
        str_mask = non_null_series.apply(lambda x: isinstance(x, str))

        floats = non_null_series[float_mask]
        ints = non_null_series[int_mask]
        bool_count = non_null_series[bool_mask].count()
        bool_count_where_true = non_null_series[bool_mask_where_true].count()
        strings = non_null_series[str_mask]
        objs = non_null_series[~(float_mask | str_mask | int_mask | bool_mask)]

        # convert numeric types to float if they are considered
        # Fractional types e.g. decimal.Decimal only if there are values
        if not floats.empty:
            floats = floats.astype(float)
            inf_mask = floats.apply(lambda x: np.isinf(x))
            self.inf_count = len(floats[inf_mask])
        self.numpy = NumpyView(floats=floats, ints=ints)
        self.pandas.strings = strings
        self.pandas.objs = objs
        self.bool_count = bool_count
        self.bool_count_where_true = bool_count_where_true

    def raw_iterator(self) -> Iterator[Any]:
        iterables = [
            *self.numpy.iterables(),
            *self.pandas.iterables(),
            *self.list.iterables(),
        ]
        return itertools.chain(iterables)

    @staticmethod
    def apply(data: Any) -> "PreprocessedColumn":
        result = PreprocessedColumn()
        result.original = data
        if pd.Series is not None and isinstance(data, pd.Series):
            result._pandas_split(data)
            result.len = len(data)
            return result

        if isinstance(data, np.ndarray):
            result.len = len(data)
            if issubclass(data.dtype.type, np.number):
                if issubclass(data.dtype.type, np.floating):
                    result.numpy = NumpyView(floats=data.astype(float))
                else:
                    result.numpy = NumpyView(ints=data.astype(int))
            else:
                raise ValueError(f"Unsupported numpy type: {data.dtype}")
            return result

        if isinstance(data, List):
            result.len = len(data)
            if is_not_stub(pd.Series):
                return PreprocessedColumn.apply(pd.Series(data, dtype="object"))

            int_list = []
            float_list = []
            string_list = []
            obj_list = []
            null_count = 0
            for x in data:
                if isinstance(x, int):
                    int_list.append(x)
                elif isinstance(x, float):
                    float_list.append(x)
                elif isinstance(x, str):
                    string_list.append(x)
                elif x is not None:
                    obj_list.append(x)
                else:
                    null_count += 1

            result.null_count = null_count
            if is_not_stub(np.ndarray):
                ints = np.asarray(int_list, dtype=int)
                floats = np.asarray(float_list, dtype=float)

                result.numpy = NumpyView(ints=ints, floats=floats)
                result.list = ListView(strings=string_list, objs=obj_list)
                return result
            else:
                result.list = ListView(ints=int_list, floats=float_list, strings=string_list, objs=obj_list)
                return result

        if isinstance(data, Iterable) or isinstance(data, Iterator):
            logger.warning(
                "Materializing an Iterable or Iterator into a list for processing. This could cause memory issue"
            )
            list_format = list(data)
            return PreprocessedColumn.apply(list_format)

        # scalars - when processing dictionary entries
        logger.info(f"Warning single value passed as column data, wrapping type: {type(data)} in list")
        list_format = [data]
        return PreprocessedColumn.apply(list_format)
