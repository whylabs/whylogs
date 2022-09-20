from whylogs.api.store.utils.random_strings import random_string


def test_default_random():
    result = random_string()

    assert len(result) == 7
    assert isinstance(result, str)
    assert result.islower()


def test_is_random():
    result_1 = random_string()
    result_2 = random_string()

    assert result_1 != result_2


def test_different_len():
    result = random_string(10)

    assert isinstance(result, str)
    assert len(result) == 10
    assert result.islower()
