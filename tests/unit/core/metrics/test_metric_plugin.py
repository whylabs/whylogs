import logging as test_logging
from dataclasses import dataclass, field

import pytest
from smart_open import open

from whylogs.v2 import MetricPlugin

TEST_LOGGER = test_logging.getLogger(__name__)

"""
An example of implementation of a custom metric to test with.
"""


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


def tests_custom_metrics_name():
    custom_metric = ACustomMetric()
    subclasses = MetricPlugin.get_subclasses()
    assert custom_metric.__class__ in subclasses
    assert isinstance(custom_metric, MetricPlugin)
    assert custom_metric.name.endswith("TargetWordCounter")


def tests_custom_metric_merge():
    custom_metric1 = ACustomMetric()
    custom_metric2 = ACustomMetric()
    expected_merge = ACustomMetric()
    custom_metric1.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric2.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    custom_metric1.merge(custom_metric2)
    expected_merge.track("ACT I SCENE I. Before LEONATO'S house.")
    expected_merge.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    assert custom_metric1.word_counts == expected_merge.word_counts
    assert custom_metric1 == expected_merge


def tests_custom_metric_merge_self():
    custom_metric = ACustomMetric()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    assert custom_metric.word_counts["ACT"] == 1
    custom_metric.merge(custom_metric)
    assert custom_metric.word_counts["ACT"] == 1


def tests_custom_metric_serialization_and_deserialization():
    custom_metric = ACustomMetric()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    serialized_metric = custom_metric.serialize()
    custom_metric_deserialized = ACustomMetric.deserialize(serialized_metric)
    assert isinstance(custom_metric_deserialized, MetricPlugin)
    assert custom_metric_deserialized == custom_metric
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    assert custom_metric_deserialized != custom_metric
    assert custom_metric.word_counts["ACT"] > 0
    assert custom_metric.word_counts["SCENE"] > 0


def tests_summary_and_serialization_are_same_by_default():
    custom_metric = ACustomMetric()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    serialized_metric = custom_metric.serialize()
    custom_metric_deserialized = ACustomMetric.deserialize(serialized_metric)
    assert isinstance(custom_metric_deserialized, MetricPlugin)
    assert custom_metric_deserialized == custom_metric
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    assert custom_metric_deserialized != custom_metric
    assert custom_metric.word_counts["ACT"] > 0
    assert custom_metric.word_counts["SCENE"] > 0


def tests_name_match_predicate():
    custom_metric = ACustomMetric()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    serialized_metric = custom_metric.serialize()
    custom_metric_deserialized = ACustomMetric.deserialize(serialized_metric)
    assert isinstance(custom_metric_deserialized, MetricPlugin)
    assert custom_metric_deserialized == custom_metric
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    assert custom_metric_deserialized != custom_metric
    assert custom_metric.word_counts["ACT"] > 0
    assert custom_metric.word_counts["SCENE"] > 0


def tests_custom_metric_protobuf_and_file_deserialization():
    custom_metric = ACustomMetric()
    custom_metric.track("ACT I SCENE I. Before LEONATO'S house.")
    custom_metric.track("Enter LEONATO, HERO, and BEATRICE, with a Messenger")
    assert custom_metric.word_counts["ACT"] == 1
    serialized_metric_bytes = custom_metric.serialize()
    deserialized_metric = MetricPlugin.deserialize(serialized_metric_bytes)
    assert isinstance(deserialized_metric, ACustomMetric)
    assert deserialized_metric == custom_metric

    # if we track more data that this metric does not care about, then the serialized version is still same
    custom_metric.track("not interesting data")
    assert deserialized_metric == custom_metric

    # if we track more data that this metric summarizes, then the serialized version is now stale
    custom_metric.track("ACT II")
    assert deserialized_metric != custom_metric
