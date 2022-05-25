import unicodedata
from typing import Dict, List, Tuple

from typing_extensions import TypeAlias

from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.metrics.metrics import DistributionMetric, OperationResult
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricMessage

ColumnSchema: TypeAlias = "ColumnSchema"  # type: ignore


DEFAULT_RANGES = {
    "emoji": (0x1F600, 0x1F64F),
    "control": (0x00, 0x1F),
    "digits": (0x30, 0x39),
    "latin-lower": (0x41, 0x5A),
    "latin-upper": (0x61, 0x7A),
    "basic-latin": (0x00, 0x7F),
    "extended-latin": (0x0080, 0x02AF),
}


_STRING_LENGTH = "string_length"


class UnicodeRangeMetric(CompoundMetric):
    """
    For string values, maintains a DistributionMetric for the counts of
    characters that fall within user-defined codepoint ranges.
    """

    range_definitions: Dict[str, Tuple[int, int]]

    def __init__(
        self,
        range_definitions: Dict[str, Tuple[int, int]] = None,
    ):
        """
        Parameters
        ----------
        range_definitions : Dict[str, Tuple[int, int]]
            Defines the character ranges to be counted. The key servers as
            the range name and should only contain alphanumeric, hyphen, and
            underscore characters. The tuple defines the Unicode codepoint
            range to be tracked. The string length is tracked under the key
            "STRING_LENGTH" so don't use that as a range name.
        """
        if range_definitions is None:
            range_definitions = DEFAULT_RANGES
        range_definitions["UNKNOWN"] = (0, 0)  # catchall for characters not in a defined range
        for key, range in range_definitions.items():
            if range[0] > range[1]:
                raise ValueError(f"Invalid codepoint range {key}")
            if range[0] < 0 or 0x10FFFF < range[1]:
                raise ValueError(f"Invalid codepoint range {key}")
            if ":" in key or "/" in key:
                raise ValueError(f"Invalid range name {key}")
        if _STRING_LENGTH in range_definitions:
            raise ValueError("STRING_LENGTH cannot be used as a range name")

        self.range_definitions = range_definitions
        from whylogs.core.schema import ColumnSchema

        submetrics = {key: DistributionMetric.zero(ColumnSchema(dtype=int)) for key in range_definitions.keys()}
        submetrics[_STRING_LENGTH] = DistributionMetric.zero(ColumnSchema(dtype=int))
        super(type(self), self).__init__(submetrics)  # type: ignore

    @property
    def namespace(self) -> str:
        return "unicode_range"

    def merge(self, other: "UnicodeRangeMetric") -> "UnicodeRangeMetric":
        submetrics = self.merge_submetrics(other)
        result = UnicodeRangeMetric(self.range_definitions)
        result.submetrics = submetrics
        return result

    def columnar_update(self, view: PreprocessedColumn) -> OperationResult:
        data = (
            view.pandas.strings.to_list() if view.pandas.strings is not None and not view.pandas.strings.empty else []
        )
        data = (data + view.list.strings) if view.list.strings else data
        range_data: Dict[str, List[int]] = {range_name: [] for range_name in self.range_definitions.keys()}
        lengths: List[int] = []
        for value in data:
            lengths.append(len(value))
            range_counter: Dict[str, int] = {range_name: 0 for range_name in self.range_definitions.keys()}
            # TODO: need to transform to utf-32 or handle surrogates
            for char in unicodedata.normalize("NFD", value).lower():
                found = False
                for range_name, range_limits in self.range_definitions.items():
                    if range_limits[0] <= ord(char) <= range_limits[1]:
                        range_counter[range_name] += 1
                        found = True
                    if not found:
                        range_counter["UNKNOWN"] += 1

            for range_name, range_count in range_counter.items():
                range_data[range_name].append(range_count)

        submetric_col = PreprocessedColumn()
        submetric_col.list.ints = lengths
        self.submetrics[_STRING_LENGTH].columnar_update(submetric_col)
        for range_name, range_list in range_data.items():
            submetric_col.list.ints = range_list
            self.submetrics[range_name].columnar_update(submetric_col)
        return OperationResult.ok(len(data))

    @classmethod
    def zero(cls, schema: ColumnSchema) -> "UnicodeRangeMetric":
        return cls(schema.cfg.unicode_ranges)

    @classmethod
    def from_protobuf(cls, msg: MetricMessage) -> "UnicodeRangeMetric":
        submetrics = cls.submetrics_from_protobuf(msg)
        # The MetricMessage doesn't contain the range definitions, so we preserve
        # the range names with empty bounds
        ranges = {sub_name: (0, 0) for sub_name in submetrics.keys()}
        del ranges[_STRING_LENGTH]
        result = cls(ranges)
        result.submetrics = submetrics
        return result
