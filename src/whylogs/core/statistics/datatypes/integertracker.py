import math

from whylogs.proto import LongsMessage


class IntTracker:
    """
    Track statistics for integers

    Parameters
    ---------
    min
        Current min value
    max
        Current max value
    sum
        Sum of the numbers
    count
        Total count of numbers
    """

    DEFAULTS = {
        "min": math.inf,
        "max": -math.inf,
        "sum": 0,
        "count": 0,
    }

    def __init__(self, min: int = None, max: int = None, sum: int = None, count: int = None):
        kwargs = locals()
        kwargs.pop("self")
        opts = {}
        opts.update(self.DEFAULTS)
        for k, v in kwargs.items():
            if v is not None:
                opts[k] = v
        self.__dict__.update(opts)

    def set_defaults(self):
        """
        Set attribute values to defaults
        """
        # NOTE: math.inf is a float, giving a possible issue with a return
        # type.  There is no max integer value in python3
        self.__dict__.update(self.DEFAULTS)

    def mean(self):
        """
        Calculate the current mean.  Returns `None` if `self.count = 0`
        """
        try:
            return self.sum / float(self.count)
        except ZeroDivisionError:
            return None

    def update(self, value):
        """
        Add a number to the tracking statistics
        """
        if value > self.max:
            self.max = value
        if value < self.min:
            self.min = value
        self.count += 1
        self.sum += value

    def merge(self, other):
        """
        Merge values of another IntTracker with this one.

        Parameters
        ----------
        other : IntTracker
            Other tracker

        Returns
        -------
        new : IntTracker
            New, merged tracker
        """
        x = IntTracker(self.min, self.max, self.sum, self.count)
        x.min = min(x.min, other.min)
        x.max = max(x.max, other.max)
        x.sum += other.sum
        x.count += other.count
        return x

    def to_protobuf(self):
        """
        Return the object serialized as a protobuf message

        Returns
        -------
        message : LongsMessage
        """
        return LongsMessage(
            count=self.count,
            max=self.max,
            min=self.min,
            sum=self.sum,
        )

    @staticmethod
    def from_protobuf(message):
        """
        Load from a protobuf message

        Returns
        -------
        number_tracker : IntTracker
        """
        return IntTracker(min=message.min, max=message.max, sum=message.sum, count=message.count)
