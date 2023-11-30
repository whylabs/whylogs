from typing import List

from whylogs.api.logger.experimental.logger.actor.list_util import (
    get_like_items,
    type_batched_items,
)


def test_get_like_items_hp() -> None:
    data = [1, 2, 3, 4, 5, "a", 6, 7, 8]
    (batch, batch_type, next) = get_like_items(data)

    assert batch == [1, 2, 3, 4, 5]
    assert isinstance(batch_type, type(int))
    assert next == ["a", 6, 7, 8]


def test_get_like_items_single_item() -> None:
    data = [1]
    (batch, batch_type, next) = get_like_items(data)

    assert batch == [1]
    assert isinstance(batch_type, type(int))
    assert next == []


def test_get_like_items_empty() -> None:
    l: List[int] = []
    (batch, batch_type, next) = get_like_items(l)

    assert batch is None
    assert batch_type is None
    assert next == []


def test_get_like_items_single_type() -> None:
    data: List[int] = [1, 2, 3, 4, 5, 6, 7, 8]
    (batch, batch_type, next) = get_like_items(data)

    assert batch == [1, 2, 3, 4, 5, 6, 7, 8]
    assert isinstance(batch_type, type(int))
    assert next == []


def test_iter() -> None:
    data = [1, 2, 3, 4, 5, "a", 6, 7, 8]
    i = 0
    for batch, batch_type in type_batched_items(data):
        if i == 0:
            assert batch == [1, 2, 3, 4, 5]
            assert isinstance(batch_type, type(int))
        elif i == 1:
            assert batch == ["a"]
            assert isinstance(batch_type, type(str))
        elif i == 2:
            assert batch == [6, 7, 8]
            assert isinstance(batch_type, type(int))
        else:
            raise Exception("Shouldn't happen")
        i += 1


def test_get_like_items_until_empty() -> None:
    data = [1, "a", 2.0]

    (batch, batch_type, next) = get_like_items(data)
    assert batch == [1]
    assert isinstance(batch_type, type(int))
    assert next == ["a", 2.0]

    (batch, batch_type, next) = get_like_items(next)
    assert batch == ["a"]
    assert isinstance(batch_type, type(str))

    assert next == [2.0]

    (batch, batch_type, next) = get_like_items(next)
    assert batch == [2.0]
    assert isinstance(batch_type, type(float))
    assert next == []
