"""
"""
import datasketches


def deserialize_kll_floats_sketch(x: bytes, kind: str = 'float'):
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
