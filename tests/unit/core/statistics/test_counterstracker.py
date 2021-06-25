from whylogs.core.statistics import CountersTracker


def test_init():
    c = CountersTracker()
    assert c.__dict__ == {"count": 0, "true_count": 0}

    opts = {"count": 1, "true_count": 3}
    c = CountersTracker(**opts)
    assert c.__dict__ == opts


def test_increment():
    c = CountersTracker()

    for _ in range(3):
        c.increment_count()

    for _ in range(2):
        c.increment_bool()

    assert c.count == 3
    assert c.true_count == 2


def test_protobuf():
    c = CountersTracker(count=1, true_count=2)
    msg = c.to_protobuf()
    c2 = CountersTracker.from_protobuf(msg)
    assert c.__dict__ == c2.__dict__


def test_simple_tracking():
    x = CountersTracker()
    x.increment_count()
    x.increment_count()
    assert x.count == 2
    assert x.true_count == 0

    x.increment_bool()
    assert x.true_count == 1


def test_merge_trackers_add_up():
    x1 = CountersTracker()
    for _ in range(2):
        x1.increment_count()
    x1.increment_bool()

    x2 = CountersTracker()
    x2.increment_count()

    merge1 = x1.merge(x2)
    assert merge1.count == 3
    assert merge1.true_count == 1

    merge2 = x2.merge(x1)
    assert merge2.count == 3
    assert merge2.true_count == 1
