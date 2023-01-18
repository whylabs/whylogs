import math
from typing import List

from whylogs.core.proto.v0 import RegressionMetricsMessage


class RegressionMetrics:
    def __init__(self):
        self.count = 0
        self.sum_abs_diff = 0.0
        self.sum_diff = 0.0
        self.sum2_diff = 0.0

    def add(self, predictions: List[float], targets: List[float]):
        """
        Function adds predictions and targets computation of regression metrics.

        Args:
            predictions (List[float]):
            targets (List[float]):
        """

        # need to vectorize this
        for idx, target in enumerate(targets):
            self.sum_abs_diff += abs(predictions[idx] - target)
            self.sum_diff += predictions[idx] - target
            self.sum2_diff += (predictions[idx] - target) ** 2
            # To add later
            # self.nt_diff.track(predictions[idx] - target)
            self.count += 1

    def mean_absolute_error(self):
        if self.count == 0:
            return None
        return self.sum_abs_diff / self.count

    def mean_squared_error(self):
        if self.count == 0:
            return None
        return self.sum2_diff / self.count

    def root_mean_squared_error(self):
        if self.count == 0:
            return None
        return math.sqrt(self.sum2_diff / self.count)

    def merge(self, other):
        """
        Merge two seperate regression metrics.

        Args:
              other : regression metrics to merge with self
        Returns:
              RegressionMetrics: merged regression metrics
        """
        if other is None:
            return self

        if self.count == 0:
            return other
        if other.count == 0:
            return self

        new_reg = RegressionMetrics()
        new_reg.count = self.count + other.count
        new_reg.sum_abs_diff = self.sum_abs_diff + other.sum_abs_diff
        new_reg.sum_diff = self.sum_diff + other.sum_diff
        new_reg.sum2_diff = self.sum2_diff + other.sum2_diff

        return new_reg

    def to_protobuf(
        self,
    ):
        """
        Convert to protobuf

        Returns:
            TYPE: Protobuf Message
        """

        return RegressionMetricsMessage(
            prediction_field="0",
            target_field="0",
            count=self.count,
            sum_abs_diff=self.sum_abs_diff,
            sum_diff=self.sum_diff,
            sum2_diff=self.sum2_diff,
        )

    @classmethod
    def from_protobuf(
        cls,
        message: RegressionMetricsMessage,
    ):
        if message.ByteSize() == 0:
            return None

        reg_met = RegressionMetrics()
        reg_met.count = message.count
        reg_met.sum_abs_diff = message.sum_abs_diff
        reg_met.sum_diff = message.sum_diff
        reg_met.sum2_diff = message.sum2_diff

        return reg_met
