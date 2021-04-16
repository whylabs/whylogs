import math

from whylogs.proto import VarianceMessage


class VarianceTracker:
    """
    Class that implements variance estimates for streaming data and for
    batched data.

    Parameters
    ----------
    count
        Number tracked elements
    sum
        Sum of all numbers
    mean
        Current estimate of the mean
    """

    def __init__(self, count=0, sum=0.0, mean=0.0):
        self.count = count
        self.sum = sum
        self.mean = mean

    def update(self, new_value):
        """
        Add a number to tracking estimates

        Based on
        https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Welford's_online_algorithm

        Parameters
        ----------
        new_value : int, float
        """
        self.count += 1

        delta = new_value - self.mean
        self.mean += delta / self.count
        delta2 = new_value - self.mean
        self.sum += delta * delta2
        return

    def stddev(self):
        """
        Return an estimate of the sample standard deviation
        """
        return math.sqrt(self.variance())

    def variance(self):
        """
        Return an estimate of the sample variance
        """
        if self.count == 0:
            return math.nan
        if self.count == 1:
            return 0
        return self.sum / (self.count - 1)

    def merge(self, other: "VarianceTracker"):
        """
        Merge statistics from another VarianceTracker into this one

        See:
        https://en.wikipedia.org/wiki/Algorithms_for_calculating_variance#Parallel_algorithm

        Parameters
        ----------
        other : VarianceTracker
            Other variance tracker

        Returns
        -------
        merged : VarianceTracker
            A new variance tracker from the merged statistics
        """
        if other.count == 0:
            return self.copy()

        if self.count == 0:
            return other.copy()

        delta = self.mean - other.mean
        total_count = self.count + other.count
        this_ratio = self.count / total_count
        other_ratio = 1.0 - this_ratio
        # Create new tracker
        this_copy = self.copy()
        this_copy.sum += other.sum + (delta ** 2) * this_copy.count * other.count / total_count
        this_copy.mean = this_copy.mean * this_ratio + other.mean * other_ratio
        this_copy.count += other.count
        return this_copy

    def copy(self):
        """
        Return a copy of this tracker
        """
        return VarianceTracker(count=self.count, sum=self.sum, mean=self.mean)

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : VarianceMessage
        """
        return VarianceMessage(count=self.count, sum=self.sum, mean=self.mean)

    @staticmethod
    def from_protobuf(message):
        """
        Load from a protobuf message

        Returns
        -------
        variance_tracker : VarianceTracker
        """
        tracker = VarianceTracker()
        tracker.count = message.count
        tracker.mean = message.mean
        tracker.sum = message.sum
        return tracker
