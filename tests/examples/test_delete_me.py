import pytest

from whylogs_v1.examples import delete_me


@pytest.mark.parametrize(
    "input_n,output_n",
    [
        (3, 2),
    ],
)
def test_decrement(input_n: int, output_n: int) -> None:
    assert delete_me.f(input_n) == output_n


def test_hllSketch() -> None:
    result = delete_me.message(13)
    assert "lg_k: 13" in str(result)
