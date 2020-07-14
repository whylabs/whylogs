"""
"""
def frequent_items_to_dict(x: list):
    d = {}
    for xi in x:
        d[xi[0]] = xi[1:]
    return d


def compare_frequent_items(x1, x2):
    assert len(x1) == len(x2)
    d1 = frequent_items_to_dict(x1)
    d2 = frequent_items_to_dict(x2)
    assert  d1 == d2
