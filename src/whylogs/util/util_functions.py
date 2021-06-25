import numpy as np


def encode_to_integers(values, uniques):
    table = {val: i for i, val in enumerate(uniques)}
    for v in values:
        if v not in uniques:
            raise ValueError("Can not encode values not in uniques")
    return np.array([table[v] for v in values])
