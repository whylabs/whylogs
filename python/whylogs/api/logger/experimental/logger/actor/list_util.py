from itertools import takewhile
from typing import Any, Generator, List, Tuple, Type


def get_like_items(items: List[Any]) -> Tuple[List[Any], Type, List[Any]]:
    if not items:
        return ([], type(None), items)

    item_type = type(items[0])
    matches = list(takewhile(lambda item: isinstance(item, item_type), items))
    return (matches, item_type, items[len(matches) :])


def type_batched_items(items: List[Any]) -> Generator[Tuple[List[Any], Type], None, None]:
    (matches, item_type, rest) = get_like_items(items)
    yield (matches, item_type)
    if not rest:
        return
    else:
        yield from type_batched_items(rest)
