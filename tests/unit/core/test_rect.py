import math
from whylogs.core.annotation_profiling import Rectangle


def test_rect():

    rect = Rectangle([[0, 0], [10, 10]], confidence=0.8,
                     labels=[{"name": "test"}])
    test = Rectangle([[0, 0], [5, 5]])
    assert rect.area == 100
    assert rect.intersection(test) == 25
    assert rect.iou(test) == 25/100.0


def test_rect():

    rect = Rectangle([[0, 0], [0, 0]])
    test = Rectangle([[0, 0], [5, 5]])
    assert rect.area == 0
    assert rect.intersection(test) == 0
    assert rect.iou(test) == 0
