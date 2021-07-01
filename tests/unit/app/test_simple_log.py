import whylogs


def test_simple_log():
    log = whylogs.log(dataset_name="test", data={"feature_name": 10})
    assert isinstance(log, whylogs.app.Logger)
    assert log.is_active()
