import whylogs_datasketches as datasketches  # type: ignore

from whylogs_v1.core.proto import HllSketchMessage


def test_hllSketch() -> None:
    test_lg_k = 13
    sketch = datasketches.hll_sketch(test_lg_k)
    result = HllSketchMessage(sketch=sketch.serialize_compact(), lg_k=test_lg_k)
    assert f"lg_k: {test_lg_k}" in str(result)
