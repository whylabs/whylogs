#!/usr/bin/env python3
"""
created 5/7/20 by ibackus

TODO: implement this using something other than yaml
"""
import yaml
from whylabs.logs.proto import InferredType
TYPES = InferredType.Type
# Dictionary mapping from type Number to type name
TYPENUM_TO_NAME = {k: v for v, k in InferredType.Type.items()}


class TypedDataConverter:
    """
    A class to coerce types on data
    """
    @staticmethod
    def convert(data):
        """
        Convert `data` to a typed value

        If a `data` is a string, parse `data` with yaml.  Else, return `data`
        unchanged
        """
        if isinstance(data, str):
            data = yaml.safe_load(data)
        return data

    @staticmethod
    def get_type(typed_data):
        """
        Extract the data type of a value.  See `typeddataconvert.TYPES` for
        available types.
        """
        dtype = TYPES.UNKNOWN
        if typed_data is None:
            dtype = TYPES.NULL
        elif isinstance(typed_data, bool):
            dtype = TYPES.BOOLEAN
        elif isinstance(typed_data, float):
            dtype = TYPES.FRACTIONAL
        elif isinstance(typed_data, int):
            dtype = TYPES.INTEGRAL
        elif isinstance(typed_data, str):
            dtype = TYPES.STRING
        return dtype
