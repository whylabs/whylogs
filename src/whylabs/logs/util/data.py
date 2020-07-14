"""
"""
from collections import OrderedDict


def getter(x, k, *args):
    """
    get an attribute (from an object) or k (from a dict-like object)

    `getter(x, k)` raise KeyError if `k` not present

    `getter(x, k, default)` return default if `k` not present
    """
    try:
        try:
            # assume x is dict-like
            val = x[k]
        except TypeError:
            # x is not dict-like, try to get an attribute
            try:
                val = getattr(x, k)
            except AttributeError:
                # Attribute not present
                raise KeyError
    except KeyError as e:
        if len(args) > 0:
            return args[0]
        else:
            raise (e)
    return val


def remap(x, mapping: dict):
    """
    Flatten a nested dictionary/object according to a specified name mapping.

    Parameters
    ----------
    x : object, dict
        An object or dict which can be treated as a nested dictionary, where
        attributes can be accessed as:
            `attr = x.a.b['key_name']['other_Name'].d`
        Indexing list values is not implemented, e.g.:
            `x.a.b[3].d['key_name']`
    mapping : dict
        Nested dictionary specifying the mapping.  ONLY values specified in the
        mapping will be returned.
        For example:
        .. code-block:: python

            {'a': {
                'b': {
                    'c': 'new_name'
                }
            }

        could flatten `x.a.b.c` or `x.a['b']['c']` to 'new_name'

    Returns
    -------
    flat : OrderedDict
        A flattened ordered dictionary of values

    """
    out = OrderedDict()
    _remap(x, mapping, out)
    return out


def _remap(x, mapping: dict, y: dict):
    for k, mapper in mapping.items():
        try:
            val = getter(x, k)
        except KeyError:
            continue

        if isinstance(mapper, dict):
            # Gotta keep walking the tree
            _remap(val, mapper, y)
        else:
            # We have found a variable we are going to re-name!
            y[mapper] = val


def get_valid_filename(s):
    """
    Return the given string converted to a string that can be used for a clean
    filename. Remove leading and trailing spaces; convert other spaces to
    underscores; and remove anything that is not an alphanumeric, dash,
    underscore, or dot.
    >>> get_valid_filename("john's portrait in 2004.jpg")
    'johns_portrpyait_in_2004.jpg'
    """
    import re
    s = str(s).strip().replace(' ', '_')
    s = s.replace('/', '-')
    return re.sub(r'(?u)[^-\w.]', '', s)
