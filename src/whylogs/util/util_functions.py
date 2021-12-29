import numpy as np


def encode_to_integers(values, uniques):
    table = {val: i for i, val in enumerate(uniques)}
    for v in values:
        if v not in uniques:
            raise ValueError("Can not encode values not in uniques")
    return np.array([table[v] for v in values])


def column_tuple_to_string(column_tuple: tuple[str]) -> str:
    """
    Rertun a string from the column pair, used as key in proto message map
    :param column_tuple: tuple, the column pair to convert to string
    """
    return str(column_tuple[0]) + " ; " + str(column_tuple[1])


def string_to_column_tuple(column_str: str) -> tuple[str]:
    """
    Rertun a tuple from the column string, used as key in constraints map
    :param column_str: str, the column string to convert to tuple
    """
    return tuple(column_str.split(" ; "))
