import datasketches

from whylogs.proto import UniqueCountSummary


def _copy_union(union):
    new_union = datasketches.theta_union()
    new_union.update(union.get_result())
    return new_union


class ThetaSketch:
    """
    A sketch for approximate cardinality tracking.

    A wrapper class for `datasketches.update_theta_sketch` which implements
    merging for updatable theta sketches.

    Currently, datasketches only implements merging for compact (read-only)
    theta sketches.
    """

    def __init__(self, theta_sketch=None, union=None, compact_theta=None):
        if theta_sketch is None:
            theta_sketch = datasketches.update_theta_sketch()
        if union is None:
            union = datasketches.theta_union()
        else:
            union = _copy_union(union)
        if compact_theta is not None:
            union.update(compact_theta)

        self.theta_sketch = theta_sketch
        self.union = union

    def update(self, value):
        """
        Update the statistics tracking

        Parameters
        ----------
        value : object
            Value to follow
        """
        self.theta_sketch.update(value)

    def merge(self, other):
        """
        Merge another `ThetaSketch` with this one, returning a new object

        Parameters
        ----------
        other : ThetaSketch
            Other theta sketch

        Returns
        -------
        new : ThetaSketch
            New theta sketch with merged statistics
        """
        new_union = datasketches.theta_union()
        new_union.update(self.get_result())
        new_union.update(other.get_result())
        return ThetaSketch(union=new_union)

    def get_result(self):
        """
        Generate a theta sketch

        Returns
        -------
        compact_sketch : datasketches.compact_theta_sketch
            Read-only compact theta sketch with full statistics.
        """
        new_union = datasketches.theta_union()
        new_union.update(self.union.get_result())
        new_union.update(self.theta_sketch)
        return new_union.get_result()

    def serialize(self):
        """
        Serialize this object.

        Note that serialization only preserves the object approximately.

        Returns
        -------
        msg : bytes
            Serialized to `bytes`
        """
        return self.get_result().serialize()

    @staticmethod
    def deserialize(msg: bytes):
        """
        Deserialize from a serialized message.

        `msg`

        Parameters
        ----------
        msg : bytes
            Serialized object.  can be a serialized version of:
                * ThetaSketch
                * datasketches.update_theta_sketch,
                * datasketches.compact_theta_sketch

        Returns
        -------
        sketch : ThetaSketch
            ThetaSketch object
        """
        theta = datasketches.compact_theta_sketch.deserialize(msg)
        if isinstance(theta, datasketches.compact_theta_sketch):
            return ThetaSketch(compact_theta=theta)
        else:
            raise ValueError(f"Unrecognized type: {type(theta)}")

    def to_summary(self, num_std_devs=1):
        """
        Generate a summary protobuf message

        Parameters
        ----------
        num_std_devs : float
            For estimating bounds

        Returns
        -------
        summary : UniqueCountSummary
            Summary protobuf message
        """
        compact_theta = self.get_result()
        return UniqueCountSummary(
            estimate=compact_theta.get_estimate(),
            upper=compact_theta.get_upper_bound(num_std_devs),
            lower=compact_theta.get_lower_bound(num_std_devs),
        )


def numbers_summary(sketch: ThetaSketch, num_std_devs=1):
    """
    Generate a summary protobuf message from a thetasketch based on numeric
    values

    Parameters
    ----------
    sketch

    num_std_devs : float
        For estimating bounds

    Returns
    -------
    summary : UniqueCountSummary
        Summary protobuf message
    """
    compact_theta = sketch.get_result()
    return UniqueCountSummary(
        estimate=compact_theta.get_estimate(),
        upper=compact_theta.get_upper_bound(num_std_devs),
        lower=compact_theta.get_lower_bound(num_std_devs),
    )
