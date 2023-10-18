from hashlib import sha1
from typing import List, Union


def encode_strings(col_names: List[Union[int, str]]) -> int:
    """
    Encode a list of strings as a number by hashing each one and then adding them together.
    This is useful for generating group keys based on string lists that don't care about order
    and have reasonably low collision rate without having to sort everything.

    Args:
        col_names: list of column names to encode. If the whylogs data doesn't specify a column name then
            it will end up being a sequential number.
    """
    # Use sha1 because its fast. This isn't used for anything related to security.
    hashes = [int.from_bytes(sha1(str(it).encode("utf-8")).digest(), "big") for it in col_names]
    return sum(hashes)
