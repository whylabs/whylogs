from concurrent.futures import Future

import pytest  # type: ignore

try:
    from whylogs.api.logger.experimental.logger.actor.future_util import (
        wait_result,
        wait_result_while,
    )
except Exception as e:
    if str(e) == "'type' object is not subscriptable":
        pytest.skip("Skipping module because of a pytest bug on older python versions.", allow_module_level=True)


def test_success() -> None:
    f: Future[int] = Future()
    f.set_result(2)
    result = wait_result(f)
    assert result == 2


def test_error() -> None:
    f: Future[int] = Future()
    e = Exception("e")
    f.set_exception(e)

    with pytest.raises(Exception):
        wait_result(f)


def test_cancel() -> None:
    f: Future[int] = Future()
    f.cancel()
    f.set_running_or_notify_cancel()

    with pytest.raises(Exception):
        wait_result(f)


def test_timeout() -> None:
    f: Future[int] = Future()
    with pytest.raises(TimeoutError):
        wait_result(f, 0.1)


def test_while_timeout() -> None:
    i = 0

    def predicate() -> bool:
        nonlocal i
        i += 1
        return i < 3

    f: Future[int] = Future()
    with pytest.raises(TimeoutError):
        wait_result_while(f, predicate)
