from whylogs.util import stats


def test_is_discrete():
    assert not stats.is_discrete(0, 0)
    assert not stats.is_discrete(0, 0, 0)
    assert not stats.is_discrete(-1, 100)
    assert not stats.is_discrete(10, 100)
    assert stats.is_discrete(1000, 3)
    assert stats.is_discrete(6, 5, p=1)
    assert not stats.is_discrete(6, 5, p=0.75)
    assert not stats.is_discrete(5, 5, p=1)
    try:
        stats.is_discrete(1, 0)
        raise RuntimeError("ValueError not raised")
    except ValueError:
        pass
