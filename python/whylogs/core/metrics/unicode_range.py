import unicodedata
from dataclasses import dataclass
from typing import Dict, List, Tuple

from whylogs.core.metrics.compound_metric import CompoundMetric
from whylogs.core.metrics.metrics import (
    DistributionMetric,
    MetricConfig,
    OperationResult,
)
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.proto import MetricMessage

_STRING_LENGTH = "string_length"


@dataclass
class UnicodeRangeMetric(CompoundMetric):
    """
    For string values, maintains a DistributionMetric for the counts of
    characters that fall within user-defined codepoint ranges.

    Parameters
     ----------
     range_definitions : Dict[str, Tuple[int, int]]
         Defines the character ranges to be counted. The key servers as
         the range name and should only contain alphanumeric, hyphen, and
         underscore characters. The tuple defines the Unicode codepoint
         range to be tracked. The string length is tracked under the key
         "STRING_LENGTH" so don't use that as a range name.
    """

    range_definitions: Dict[str, Tuple[int, int]]
    lower_case: bool = True
    normalize: bool = True

    def __post_init__(self):
        super(type(self), self).__post_init__()
        self.range_definitions["UNKNOWN"] = (0, 0)  # catchall for characters not in a defined range
        for key, range in self.range_definitions.items():
            if range[0] > range[1]:
                raise ValueError(f"Invalid codepoint range {key}")
            if range[0] < 0 or 0x10FFFF < range[1]:
                raise ValueError(f"Invalid codepoint range {key}")
            if ":" in key or "/" in key:
                raise ValueError(f"Invalid range name {key}")
        if _STRING_LENGTH in self.range_definitions:
            raise ValueError("STRING_LENGTH cannot be used as a range name")

        submetrics = {
            key: DistributionMetric.zero(MetricConfig(large_kll_k=False)) for key in self.range_definitions.keys()
        }
        submetrics[_STRING_LENGTH] = DistributionMetric.zero(MetricConfig(large_kll_k=False))
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
            s = unicodedata.normalize("NFD", value) if self.normalize else value
            s = s.lower() if self.lower_case else s
            for char in s:
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
    def zero(cls, config: MetricConfig) -> "UnicodeRangeMetric":
        return cls(config.unicode_ranges, lower_case=config.lower_case, normalize=config.normalize)

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
