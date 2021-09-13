import logging as test_logging
from dataclasses import dataclass, field

import pytest
from dataclasses_json import dataclass_json
from smart_open import open

from whylogs.v2 import MetricPlugin

TEST_LOGGER = test_logging.getLogger(__name__)

"""
An example of implementation of a custom metric to test with.
"""


@dataclass_json
@dataclass
class ACustomMetric(MetricPlugin):
    name: str = "TargetWordCounter"
    target_column_name: str = "Much Ado About Nothing"
    word_counts: dict = field(default_factory=lambda: {"ACT": 0, "SCENE": 0, "BEATRICE": 0, "HERO": 0, "BENEDICK": 0})

    def track(self, data):
        for target_string in self.word_counts.keys():
            if target_string in data:
                self.word_counts[target_string] = self.word_counts[target_string] + data.count(target_string)

    def merge(self, other: "ACustomMetric"):
        if other is None or other is self:
            return self
        # Custom metrics can define how they merge, here we add counts of any matching words.
        words = self.word_counts.keys() | other.word_counts.keys()
        for word in words:
            if self.word_counts[word]:
                if other.word_counts[word]:
                    self.word_counts[word] = self.word_counts[word] + other.word_counts[word]
            else:
                self.word_counts[word] = other.word_counts[word]


@dataclass_json
@dataclass
class CustomMetricSerializedAsJson(ACustomMetric):
    name: str = "TargetWordCounterJson"

    @staticmethod
    def deserialize(data: bytes) -> "CustomMetricSerializedAsJson":
        return MetricPlugin.from_string(data)

    def serialize(self) -> bytes:
        return self.to_string()


@dataclass_json
@dataclass
class CustomMetricWithPrivateField(CustomMetricSerializedAsJson):
    name: str = "TargetWordCounterPrivateField"
    a_field: int = 0
    __private_field: int = 0


@dataclass_json
@dataclass
class CustomMetricNested(MetricPlugin):
    custom_metric: ACustomMetric = field(default_factory=ACustomMetric)
    name: str = "TargetWordCounterNested"
    target_column_name: str = "Much Ado About Nothing"
    is_container: bool = True

    def track(self, data):
        self.custom_metric.track(data)

    def merge(self, other: "CustomMetricNested"):
        self.custom_metric.merge(other.custom_metric)

    @property
    def word_counts(self):
        return self.custom_metric.word_counts


@dataclass_json
@dataclass
class DoubleNested(MetricPlugin):
    custom_metric: CustomMetricNested = field(default_factory=CustomMetricNested)
    name: str = "TargetWordCounterDoubleNested"
    target_column_name: str = "Much Ado About Nothing"
    is_container: bool = True

    def track(self, data):
        self.custom_metric.track(data)

    def merge(self, other: "DoubleNested"):
        self.custom_metric.merge(other.custom_metric)

    @property
    def word_counts(self):
        return self.custom_metric.word_counts


testdata = [ACustomMetric, CustomMetricSerializedAsJson, CustomMetricWithPrivateField, CustomMetricNested, DoubleNested]


@pytest.mark.parametrize("plugin_type", testdata)
def tests_custom_metrics_name(plugin_type):
    custom_metric = plugin_type()
    subclasses = MetricPlugin.get_subclasses()
    assert custom_metric.__class__ in subclasses
    assert isinstance(custom_metric, MetricPlugin)
    assert custom_metric.name.startswith("TargetWordCounter")


@pytest.mark.parametrize("plugin_type", testdata)
def tests_custom_metric_merge(plugin_type):
    custom_metric1 = plugin_type()
    custom_metric2 = plugin_type()
    expected_merge = plugin_type()
    custom_metric1.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric2.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    custom_metric1.merge(custom_metric2)
    expected_merge.track("ACT I SCENE I. Before LEONATO'S house.")
    expected_merge.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    assert custom_metric1.word_counts == expected_merge.word_counts
    assert custom_metric1 == expected_merge


@pytest.mark.parametrize("plugin_type", testdata)
def tests_custom_metric_merge_self(plugin_type):
    custom_metric = plugin_type()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    assert custom_metric.word_counts["ACT"] == 1, f"{custom_metric}"
    custom_metric.merge(custom_metric)
    assert custom_metric.word_counts["ACT"] == 1, f"{custom_metric}"


@pytest.mark.parametrize("plugin_type", testdata)
def tests_custom_metric_serialization_and_deserialization(plugin_type):
    custom_metric = plugin_type()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    serialized_metric = custom_metric.serialize()
    custom_metric_deserialized = plugin_type.deserialize(serialized_metric)
    assert isinstance(custom_metric_deserialized, plugin_type)
    assert custom_metric_deserialized == custom_metric
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    assert custom_metric_deserialized != custom_metric
    assert custom_metric.word_counts["ACT"] > 0
    assert custom_metric.word_counts["SCENE"] > 0


@pytest.mark.parametrize("plugin_type", testdata)
def tests_summary_and_serialization_are_same_by_default(plugin_type):
    custom_metric = plugin_type()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    serialized_metric = custom_metric.serialize()
    custom_metric_deserialized = plugin_type.deserialize(serialized_metric)
    assert isinstance(custom_metric_deserialized, MetricPlugin)
    assert custom_metric_deserialized == custom_metric
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    assert custom_metric_deserialized != custom_metric
    assert custom_metric.word_counts["ACT"] > 0
    assert custom_metric.word_counts["SCENE"] > 0


@pytest.mark.parametrize("plugin_type", testdata)
def tests_name_match_predicate(plugin_type):
    custom_metric = plugin_type()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    serialized_metric = custom_metric.serialize()
    custom_metric_deserialized = plugin_type.deserialize(serialized_metric)
    assert isinstance(custom_metric_deserialized, MetricPlugin)
    assert custom_metric_deserialized == custom_metric
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    assert custom_metric_deserialized != custom_metric
    assert custom_metric.word_counts["ACT"] > 0
    assert custom_metric.word_counts["SCENE"] > 0


@pytest.mark.parametrize("plugin_type", testdata)
def tests_custom_metric_protobuf_and_file_deserialization(plugin_type):
    write_test_files = False
    custom_metric = plugin_type()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    assert custom_metric.word_counts["ACT"] == 1
    serialized_metric_bytes = custom_metric.serialize()
    test_file_name = f"metric.plugin.{plugin_type.__name__}.bin"
    if write_test_files:
        if isinstance(serialized_metric_bytes, str):
            with open(test_file_name, "w") as output_file:
                output_file.write(serialized_metric_bytes)
        else:
            with open(test_file_name, "wb") as output_file:
                output_file.write(serialized_metric_bytes)

    deserialized_metric = plugin_type.deserialize(serialized_metric_bytes)
    assert isinstance(deserialized_metric, plugin_type)
    assert deserialized_metric == custom_metric

    # if we track more data that this metric does not care about, then the serialized version is still same
    custom_metric.track("not interesting data")
    assert deserialized_metric == custom_metric

    # if we track more data that this metric summarizes, then the serialized version is now stale
    custom_metric.track("ACT II")
    assert deserialized_metric != custom_metric


def tests_custom_metric_json_deserialization():
    custom_metric = CustomMetricSerializedAsJson()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    assert custom_metric.word_counts["ACT"] == 1
    serialized_metric_bytes = custom_metric.to_string()
    TEST_LOGGER.info(serialized_metric_bytes)
    deserialized_metric = CustomMetricSerializedAsJson.from_string(serialized_metric_bytes)
    assert isinstance(deserialized_metric, CustomMetricSerializedAsJson)
    assert deserialized_metric == custom_metric

    # if we track more data that this metric does not care about, then the serialized version is still same
    custom_metric.track("not interesting data")
    assert deserialized_metric == custom_metric

    # if we track more data that this metric summarizes, then the serialized version is now stale
    custom_metric.track("ACT II")
    assert deserialized_metric != custom_metric
