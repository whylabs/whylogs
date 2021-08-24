#!/usr/bin/env python3
"""
TODO: implement this using something other than yaml
"""
import numpy as np
import pandas as pd
import yaml

from whylogs.proto import InferredType

TYPES = InferredType.Type
# Dictionary mapping from type Number to type name
TYPENUM_TO_NAME = {k: v for v, k in InferredType.Type.items()}
INTEGRAL_TYPES = (int, np.integer)
FLOAT_TYPES = (float, np.float)


class TypedDataConverter:
    """
    A class to coerce types on data.

    To see available types:

    .. code-block:: python

        >>> from whylogs.core.types.typeddataconverter import TYPES
        >>> print("\\n".join(sorted(TYPES.keys())))
    """

    @staticmethod
    def convert(data):
        """
        Convert `data` to a typed value

        If a `data` is a string, parse `data` with yaml.  Else, return `data`
        unchanged

        Note: this method is very slow, since it relies on the complex and
        python-based implementation of yaml.
        """
        if isinstance(data, str):
            try:
                data = yaml.safe_load(data)
            except Exception:
                # Obviously this is very bad coding practice to catch all
                # exceptions, but if we can't parse data with yaml, then it's
                # not yaml data, therefore I'll call it a string!
                pass
        return data

    @staticmethod
    def get_type(typed_data):
        """
        Extract the data type of a value.  See `typeddataconvert.TYPES` for
        available types.

        Parameters
        ----------
        typed_data
            Data processed by TypedDataConverter.convert

        Returns
        -------
        dtype : TYPES
        """
        dtype = TYPES.UNKNOWN
        if pd.isnull(typed_data):
            dtype = TYPES.NULL
        elif isinstance(typed_data, bool):
            dtype = TYPES.BOOLEAN
        elif isinstance(typed_data, FLOAT_TYPES):
            dtype = TYPES.FRACTIONAL
        elif isinstance(typed_data, INTEGRAL_TYPES):
            dtype = TYPES.INTEGRAL
        elif isinstance(typed_data, str):
            dtype = TYPES.STRING
        return dtype
