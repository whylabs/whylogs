"""
"""
import datasketches
import json

from whylabs.logs.proto import FrequentNumbersSummary, FrequentNumbersSketchMessage
from collections import defaultdict


def deserialize_kll_floats_sketch(x: bytes, kind: str='float'):
    """
    Deserialize a KLL floats sketch.  Compatible with WhyLogs-Java

    WhyLogs histograms are serialized as kll floats sketches

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
    if kind == 'float':
        h = datasketches.kll_floats_sketch.deserialize(x)
    elif kind == 'int':
        h = datasketches.kll_ints_sketch(x)
    if h.get_n() < 1:
        return
    return h


def deserialize_frequent_strings_sketch(x: bytes):
    """
    Deserialize a frequent strings sketch.  Compatible with WhyLogs-Java

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


class FrequentNumbersSketch:
    """
    A class to implement frequent number counting.

    Wraps datasketches.frequent_strings_sketch by encoding numbers as strings.
    """
    DEFAULT_MAX_ITEMS_SIZE = 32
    DEFAULT_ERROR_TYPE = datasketches.frequent_items_error_type.NO_FALSE_NEGATIVES

    def __init__(self, lg_max_k: int=None,
                 sketch: datasketches.frequent_strings_sketch=None):
        self.lg_max_k = lg_max_k
        if sketch is None:
            if lg_max_k is None:
                lg_max_k = self.DEFAULT_MAX_ITEMS_SIZE
            sketch = datasketches.frequent_strings_sketch(lg_max_k)
        else:
            assert isinstance(sketch, datasketches.frequent_strings_sketch)
        self.sketch = sketch


    def get_apriori_error(self, lg_max_map_size: int,
                          estimated_total_weight: int):
        return self.sketch.get_apriori_error(
            lg_max_map_size, estimated_total_weight)

    def get_epsilon_for_lg_size(self, lg_max_map_size: int):
        return self.sketch.get_epsilon_for_lg_size(lg_max_map_size)

    def get_estimate(self, item):
        return self.sketch.get_estimate(self._encode_number(item))

    def get_lower_bound(self, item):
        return self.sketch.get_lower_bound(self._encode_number(item))

    def get_upper_bound(self, item):
        return self.sketch.get_upper_bound(self._encode_number(item))

    def get_frequent_items(
            self,
            err_type: datasketches.frequent_items_error_type=None,
            threshold: int=0):
        if err_type is None:
            err_type = self.DEFAULT_ERROR_TYPE
        items = self.sketch.get_frequent_items(err_type, threshold)
        numeric_items = []
        for item in items:
            x = self._decode_number(item[0])
            numeric_items.append((x, ) + item[1:])
        return numeric_items

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

        """
        # We want all our "merge" methods to return a NEW object.
        # TODO: investigate relaxing this constraint
        self_copy = self.copy()
        self_copy.sketch.merge(other.sketch)
        return self_copy

    def copy(self):
        self_copy = FrequentNumbersSketch.deserialize(self.serialize())
        if self_copy is None:
            # Self must be empty
            self_copy = FrequentNumbersSketch(self.lg_max_k)
        return self_copy

    def serialize(self):
        return self.sketch.serialize()

    def to_string(self, print_items=False):
        return self.sketch.to_string(print_items)

    def update(self, x):
        self.sketch.update(self._encode_number(x))

    def to_summary(self, max_items=30, min_count=1):
        """
        Generate a protobuf summary.  Returns None if there are no frequent
        items.
        """
        items = self.get_frequent_items(threshold=min_count-1)
        if len(items) < 1:
            return

        items_dict = defaultdict(list)
        for rank, x in enumerate(items[0:max_items]):
            d = {'value': x[0], 'estimate': x[1], 'rank': rank}
            if isinstance(d['value'], float):
                items_dict['doubles'].append(d)
            else:
                items_dict['longs'].append(d)

        return FrequentNumbersSummary(**items_dict)

    def to_protobuf(self):
        """
        Generate a protobuf representation of this object
        """
        lg_max_k = self.lg_max_k
        if lg_max_k is None:
            lg_max_k = -1
        return FrequentNumbersSketchMessage(
            sketch=self.sketch.serialize(),
            lg_max_k=lg_max_k,
        )

    @staticmethod
    def from_protobuf(message: FrequentNumbersSketchMessage):
        lg_max_k = message.lg_max_k
        if lg_max_k < 0:
            lg_max_k = None
        sketch = FrequentNumbersSketch.deserialize(message.sketch)
        sketch.lg_max_k = lg_max_k
        return sketch

    @staticmethod
    def _encode_number(x):
        return json.dumps(x)

    @staticmethod
    def _decode_number(x):
        return json.loads(x)

    @staticmethod
    def deserialize(x: bytes):
        """
        Deserialize a frequent numbers sketch.

        If x is an empty sketch, None is returned
        """
        if len(x) <= 8:
            FrequentNumbersSketch()
        sketch = datasketches.frequent_strings_sketch.deserialize(x)
        return FrequentNumbersSketch(sketch=sketch)

    @staticmethod
    def flatten_summary(summary: FrequentNumbersSummary):
        """
        Flatten a FrequentNumbersSummary
        """
        counts = {'value': [], 'count': []}
        for num_list in (summary.doubles, summary.longs):
            for msg in num_list:
                counts['value'].append(msg.value)
                counts['count'].append(msg.estimate)
        return counts