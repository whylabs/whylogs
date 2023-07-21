import itertools
import logging
from dataclasses import dataclass
from decimal import Decimal
from enum import Enum
from math import isinf, isnan
from typing import Any, Iterable, Iterator, List, Optional, Tuple, Union

from whylogs.core.stubs import is_not_stub, np, pd

logger = logging.getLogger("whylogs.core.views")

try:
    import pandas.core.dtypes.common as pdc
except:  # noqa
    pass


class ColumnProperties(Enum):
    default = 0
    homogeneous = 1


@dataclass
class ListView:
    ints: Optional[List[int]] = None
    floats: Optional[List[Union[float, Decimal]]] = None
    strings: Optional[List[str]] = None
    tensors: Optional[List[np.ndarray]] = None
    objs: Optional[List[Any]] = None

    def iterables(self) -> List[List[Any]]:
        it_list = []
        for lst in [self.ints, self.floats, self.strings, self.tensors, self.objs]:
            if lst is not None and len(lst) > 0:
                it_list.append(lst)
        return it_list


@dataclass
class NumpyView:
    ints: Optional[np.ndarray] = None
    floats: Optional[np.ndarray] = None
    strings: Optional[np.ndarray] = None

    def iterables(self) -> List[np.ndarray]:
        it_list = []
        for lst in [self.ints, self.floats, self.strings]:
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
        if self.strings is not None:
            length += len(self.strings)

        return length


@dataclass
class PandasView:
    strings: Optional[pd.Series] = None
    tensors: Optional[pd.Series] = None
    objs: Optional[pd.Series] = None

    def iterables(self) -> List[pd.Series]:
        it_list = []
        for lst in [self.strings, self.tensors, self.objs]:
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
                self.numpy.floats = floats.to_numpy()
                return
            else:
                ints = non_null_series.astype(int)
                self.numpy.ints = ints.to_numpy()
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

        # TODO: Do we want to parse numeric strings inside of tensors?

        float_mask = non_null_series.apply(lambda x: pdc.is_float(x) or pdc.is_decimal(x))
        bool_mask = non_null_series.apply(lambda x: pdc.is_bool(x))
        bool_mask_where_true = non_null_series.apply(lambda x: pdc.is_bool(x) and x)
        int_mask = non_null_series.apply(lambda x: pdc.is_number(x) and pdc.is_integer(x) and not pdc.is_bool(x))
        str_mask = non_null_series.apply(lambda x: isinstance(x, str))
        tensor_mask = non_null_series.apply(
            lambda x: isinstance(x, (list, np.ndarray)) and PreprocessedColumn._is_tensorable(x)
        )

        floats = non_null_series[float_mask]
        if non_null_series[int_mask].empty:
            ints = pd.Series(dtype=int)
        else:
            ints = non_null_series[int_mask].astype(int)
        bool_count = non_null_series[bool_mask].count()
        bool_count_where_true = non_null_series[bool_mask_where_true].count()
        strings = non_null_series[str_mask]
        tensors = non_null_series[tensor_mask]
        tensors = pd.Series([x if isinstance(x, np.ndarray) else np.asarray(x) for x in tensors], dtype="object")
        objs = non_null_series[~(float_mask | str_mask | int_mask | bool_mask | tensor_mask)]

        # convert numeric types to float if they are considered
        # Fractional types e.g. decimal.Decimal only if there are values
        if not floats.empty:
            floats = floats.astype(float)
            inf_mask = floats.apply(lambda x: np.isinf(x))
            self.inf_count = len(floats[inf_mask])

        self.numpy = NumpyView(floats=floats.to_numpy(), ints=ints.to_numpy())
        self.pandas.strings = strings
        self.pandas.tensors = tensors
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
    def _process_scalar_value(value: Any) -> "PreprocessedColumn":
        result = PreprocessedColumn()
        result.original = value
        result.len = 1
        int_list = []
        float_list = []
        string_list = []
        tensor_list = []
        obj_list = []
        int_types: Union[type, Tuple[type, type]] = (int, np.integer) if is_not_stub(np.integer) else int
        if isinstance(value, int_types):
            if isinstance(value, bool):
                result.bool_count = 1
                if value:
                    result.bool_count_where_true = 1
            else:
                int_list.append(value)
        elif isinstance(value, (float, Decimal)):
            if isinf(value):
                result.inf_count = 1
            elif isnan(value):
                result.nan_count = 1
                result.null_count = 1
            float_list.append(value)
        elif isinstance(value, str):
            string_list.append(value)
        elif isinstance(value, list) and PreprocessedColumn._is_tensorable(value):
            tensor_list.append(np.asarray(value))
        elif is_not_stub(np.ndarray) and PreprocessedColumn._is_tensorable(value):
            tensor_list.append(value)
        elif value is not None:
            obj_list.append(value)
        else:
            result.null_count = 1

        if is_not_stub(np.ndarray):
            ints = np.asarray(int_list, dtype=int)
            floats = np.asarray(float_list, dtype=float)
            if isinstance(value, np.bool_):
                result.bool_count = 1
                if value:
                    result.bool_count_where_true = 1

            result.numpy = NumpyView(ints=ints, floats=floats)
            result.list = ListView(strings=string_list, tensors=tensor_list, objs=obj_list)
        else:
            result.list = ListView(
                ints=int_list, floats=float_list, strings=string_list, tensors=tensor_list, objs=obj_list
            )

        return result

    @staticmethod
    def _process_homogeneous_column(series: pd.Series) -> "PreprocessedColumn":
        """
        Column must be of homogeneous type. NaN, None, other missing data not allowed.
        """

        result = PreprocessedColumn()
        result.original = series
        result.len = len(series)
        if series.empty:
            return result

        if pdc.is_numeric_dtype(series.dtype) and not pdc.is_bool_dtype(series.dtype):
            if pdc.is_float_dtype(series.dtype):
                result.numpy.floats = series.astype(float)
                return result
            else:
                result.numpy.ints = series.astype(int)
                return result

        # This code path is faster than _pandas_split() because it only does type
        # checking on the first value of the column. It assumes all the values are
        # the same type and none are missing.
        value = series[0]
        if isinstance(value, str):
            result.pandas.strings = series
            return result
        elif pdc.is_bool(value):
            bool_mask_where_true = series.apply(lambda x: x)
            result.bool_count = series.count()
            result.bool_count_where_true = series[bool_mask_where_true].count()
            return result
        elif isinstance(value, (list, np.ndarray)) and PreprocessedColumn._is_tensorable(value):
            if isinstance(value, np.ndarray):
                result.pandas.tensors = series
            else:
                result.pandas.tensors = pd.Series([np.asarray(x) for x in series])
            return result
        else:
            result.pandas.objs = series

        return result

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
            if issubclass(data.dtype.type, (np.number, np.str_)):
                if issubclass(data.dtype.type, np.floating):
                    result.numpy = NumpyView(floats=data.astype(float))
                elif issubclass(data.dtype.type, np.integer):
                    result.numpy = NumpyView(ints=data.astype(int))
                elif issubclass(data.dtype.type, np.str_):
                    result.numpy = NumpyView(strings=data)
            else:
                raise ValueError(f"Unsupported numpy type: {data.dtype}")
            return result

        if isinstance(data, List):
            result.len = len(data)
            if is_not_stub(pd.Series):
                return PreprocessedColumn.apply(pd.Series(data, dtype="object"))

            int_list = []
            float_list: List[Union[float, Decimal]] = []
            string_list = []
            tensor_list = []
            obj_list = []
            null_count = 0
            for x in data:
                if isinstance(x, int):
                    int_list.append(x)
                elif isinstance(x, float):
                    float_list.append(x)
                elif isinstance(x, str):
                    string_list.append(x)
                elif isinstance(x, list) and PreprocessedColumn._is_tensorable(x):
                    tensor_list.append(np.asarray(x))
                elif isinstance(x, np.ndarray) and PreprocessedColumn._is_tensorable(x):
                    tensor_list.append(x)
                elif x is not None:
                    obj_list.append(x)
                else:
                    null_count += 1

            result.null_count = null_count
            if is_not_stub(np.ndarray):
                ints = np.asarray(int_list, dtype=int)
                floats = np.asarray(float_list, dtype=float)

                result.numpy = NumpyView(ints=ints, floats=floats)
                result.list = ListView(strings=string_list, tensors=tensor_list, objs=obj_list)
                return result
            else:
                result.list = ListView(
                    ints=int_list, floats=float_list, strings=string_list, tensors=tensor_list, objs=obj_list
                )
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

    @staticmethod
    def _is_tensorable(value: Union[np.ndarray, List[Any]]) -> bool:
        if not is_not_stub(np.ndarray):
            return False

        maybe_tensor = value if isinstance(value, np.ndarray) else np.asarray(value)
        return (
            len(maybe_tensor.shape) > 0
            and all([i > 0 for i in maybe_tensor.shape])
            and np.issubdtype(maybe_tensor.dtype, np.number)
        )
