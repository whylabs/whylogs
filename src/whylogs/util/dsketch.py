"""
Define functions and classes for interfacing with `datasketches`
"""
import math

import datasketches

from whylogs.proto import FrequentItemsSketchMessage, FrequentItemsSummary


def deserialize_kll_floats_sketch(x: bytes, kind: str = "float"):
    """
    Deserialize a KLL floats sketch.  Compatible with whylogs-java

    whylogs histograms are serialized as kll floats sketches

    Parameters
    ----------
    x : bytes
        Serialized sketch
    kind : str, optional
        Specify type of sketch: 'float' or 'int'

    Returns
    -------
    sketch : `kll_floats_sketch`, `kll_ints_sketch`, or None
        If `x` is an empty sketch, return None, else return the deserialized
        sketch.
    """
    if len(x) < 1:
        return
    if kind == "float":
        h = datasketches.kll_floats_sketch.deserialize(x)
    elif kind == "int":
        h = datasketches.kll_ints_sketch(x)
    if h.get_n() < 1:
        return
    return h


def deserialize_frequent_strings_sketch(x: bytes):
    """
    Deserialize a frequent strings sketch.  Compatible with whylogs-java

    Wrapper for `datasketches.frequent_strings_sketch.deserialize`

    Parameters
    ----------
    x : bytes
        Serialized sketch

    Returns
    -------
    sketch : `datasketches.frequent_strings_sketch`, None
        If `x` is an empty string sketch, returns None, else returns the
        deserialized string sketch
    """
    if len(x) <= 8:
        return
    else:
        return datasketches.frequent_strings_sketch.deserialize(x)


class FrequentItemsSketch:
    """
    A class to implement frequent item counting for mixed data types.

    Wraps `datasketches.frequent_strings_sketch` by encoding numbers as
    strings since the `datasketches` python implementation does not implement
    frequent number tracking.

    Parameters
    ----------
    lg_max_k : int, optional
        Parameter controlling the size and accuracy of the sketch.  A larger
        number increases accuracy and the memory requirements for the sketch
    sketch : datasketches.frequent_strings_sketch, optional
        Initialize with an existing frequent strings sketch
    """

    DEFAULT_MAX_ITEMS_SIZE = 128
    DEFAULT_ERROR_TYPE = datasketches.frequent_items_error_type.NO_FALSE_NEGATIVES

    def __init__(self, lg_max_k: int = None, sketch: datasketches.frequent_strings_sketch = None):
        self.lg_max_k = lg_max_k
        if sketch is None:
            if lg_max_k is None:
                lg_max_k = round(math.log(self.DEFAULT_MAX_ITEMS_SIZE))
            sketch = datasketches.frequent_strings_sketch(lg_max_k)
        else:
            assert isinstance(sketch, datasketches.frequent_strings_sketch)
        self.sketch = sketch

    def get_apriori_error(self, lg_max_map_size: int, estimated_total_weight: int):
        """
        Return an apriori estimate of the uncertainty for various parameters

        Parameters
        ----------
        lg_max_map_size : int
            The `lg_max_k` value
        estimated_total_weight
            Total weight (see :func:`FrequentItems.get_total_weight`)
        Returns
        -------
        error : float
            Approximate uncertainty
        """
        return self.sketch.get_apriori_error(lg_max_map_size, estimated_total_weight)

    def get_epsilon_for_lg_size(self, lg_max_map_size: int):
        return self.sketch.get_epsilon_for_lg_size(lg_max_map_size)

    def get_estimate(self, item):
        return self.sketch.get_estimate(self._encode_item(item))

    def get_lower_bound(self, item):
        return self.sketch.get_lower_bound(self._encode_item(item))

    def get_upper_bound(self, item):
        return self.sketch.get_upper_bound(self._encode_item(item))

    def get_frequent_items(
        self,
        err_type: datasketches.frequent_items_error_type = None,
        threshold: int = 0,
        decode: bool = True,
    ):
        """
        Retrieve the frequent items.


        Parameters
        ----------
        err_type : datasketches.frequent_items_error_type
            Override default error type
        threshold : int
            Minimum count for returned items
        decode : bool (default=True)
            Decode the returned values.  Internally, all items are encoded
            as strings.

        Returns
        -------
        items : list
            A list of tuples of items: ``[(item, count)]``
        """
        if err_type is None:
            err_type = self.DEFAULT_ERROR_TYPE
        items = self.sketch.get_frequent_items(err_type, threshold)
        if decode:
            decoded_items = []
            for item in items:
                x = item[0]
                decoded_items.append((x,) + item[1:])
            return decoded_items
        else:
            return items

    def get_num_active_items(self):
        return self.sketch.get_num_active_items()

    def get_serialized_size_bytes(self):
        return self.sketch.get_serialized_size_bytes()

    def get_sketch_epsilon(self):
        return self.sketch.get_sketch_epsilon()

    def get_total_weight(self):
        return self.sketch.get_total_weight()

    def is_empty(self):
        return self.sketch.is_empty()

    def merge(self, other):
        """
        Merge the item counts of this sketch with another.

        This object will not be modified.  This operation is commutative.

        Parameters
        ----------
        other: FrequentItemsSketch
            The other sketch
        """
        # We want all our "merge" methods to return a NEW object.
        # TODO: investigate relaxing this constraint
        self_copy = self.copy()
        self_copy.sketch.merge(other.sketch)
        return self_copy

    def copy(self):
        """
        Returns
        -------
        sketch : FrequentItemsSketch
            A copy of this sketch
        """
        self_copy = FrequentItemsSketch.deserialize(self.serialize())
        if self_copy is None:
            # Self must be empty
            self_copy = FrequentItemsSketch(self.lg_max_k)
        return self_copy

    def serialize(self):
        """
        Serialize this sketch as a bytes string.

        See also :func:`FrequentItemsSketch.deserialize`

        Returns
        -------
        data : bytes
            Serialized object.
        """
        return self.sketch.serialize()

    def to_string(self, print_items=False):
        return self.sketch.to_string(print_items)

    def update(self, x, weight=1):
        """
        Track an item.

        Parameters
        ----------
        x : object
            Item to track
        weight : int
            Number of times the item appears
        """
        self.sketch.update(self._encode_item(x), weight)

    def to_summary(self, max_items=30, min_count=1):
        """
        Generate a protobuf summary.  Returns None if there are no frequent
        items.

        Parameters
        ----------
        max_items : int
            Maximum number of items to return.  The most frequent items will
            be returned
        min_count : int
            Minimum number counts for all returned items

        Returns
        -------
        summary : FrequentItemsSummary
            Protobuf summary message
        """
        items = self.get_frequent_items(threshold=min_count - 1, decode=False)
        if len(items) < 1:
            return

        values = []
        for x in items[0:max_items]:
            values.append({"estimate": x[1], "json_value": x[0]})
        return FrequentItemsSummary(items=values)

    def to_protobuf(self):
        """
        Generate a protobuf representation of this object
        """
        lg_max_k = self.lg_max_k
        if lg_max_k is None:
            lg_max_k = -1
        return FrequentItemsSketchMessage(
            sketch=self.sketch.serialize(),
            lg_max_k=lg_max_k,
        )

    @staticmethod
    def from_protobuf(message: FrequentItemsSketchMessage):
        """
        Initialize a FrequentItemsSketch from a protobuf
        FrequentItemsSketchMessage
        """
        lg_max_k = message.lg_max_k
        if lg_max_k < 0:
            lg_max_k = None
        sketch = FrequentItemsSketch.deserialize(message.sketch)
        sketch.lg_max_k = lg_max_k
        return sketch

    @staticmethod
    def _encode_item(x):
        return str(x)

    @staticmethod
    def deserialize(x: bytes):
        """
        Deserialize a frequent numbers sketch.

        If x is an empty sketch, None is returned
        """
        if len(x) <= 8:
            return FrequentItemsSketch()
        sketch = datasketches.frequent_strings_sketch.deserialize(x)
        return FrequentItemsSketch(sketch=sketch)
