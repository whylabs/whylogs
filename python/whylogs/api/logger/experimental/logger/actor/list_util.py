from itertools import takewhile
from typing import Generator, List, Optional, Tuple, Type, TypeVar

K = TypeVar("K")


def get_like_items(items: List[K]) -> Tuple[Optional[List[K]], Optional[Type[K]], List[K]]:
    """
    Given a list of items, return a tuple of:

    - A list of the items that are of the same type. It will take items from the input until it encounters an item that is not of the same
      type.
    - The type of the items in the first list
    - The remaining items in the list

    This is most useful when K is a union type but also works when K is a single type, or just an optional.
    """
    if not items:
        return (None, None, items)

    item_type = type(items[0])
    matches = list(takewhile(lambda item: isinstance(item, item_type), items))
    return (matches, item_type, items[len(matches) :])


def type_batched_items(items: List[K]) -> Generator[Tuple[Optional[List[K]], Optional[Type[K]]], None, None]:
    """
    Given a list of items, return a generator that yields tuples. This is a generator version of
    get_like_items and has the same semantics.

    """
    (matches, item_type, rest) = get_like_items(items)
    yield (matches, item_type)
    if not rest:
        return
    else:
        yield from type_batched_items(rest)
