import datasketches

from whylogs_v1.proto import HllSketchMessage


def user_input() -> str:
    """Accept user input for decrement function
    ---
    - Accept a number from user input.
    - Throw exception if user inputs non-numeric.
    """
    try:
        n = input("Please enter a number: ")
        return f"Decrement number {n} is: {f(int(n))}."
    except Exception as e:
        raise e


def f(n: int) -> int:
    """Return n decremented."""
    return n - 1


def message(lg_k: int) -> HllSketchMessage:
    """Create an empy proto message and return it."""
    sketch = datasketches.hll_sketch(lg_k)
    return HllSketchMessage(sketch=sketch.serialize_compact(), lg_k=lg_k)


if __name__ == "__main__":
    print(user_input())
