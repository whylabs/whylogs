from concurrent.futures import Future

import pytest

try:
    from whylogs.api.logger.experimental.logger.actor.future_util import wait_result
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
