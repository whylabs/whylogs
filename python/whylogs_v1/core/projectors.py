from typing import Any, Dict


class SingleFieldProjector(object):
    def __init__(self, col_name: str) -> None:
        self._col_name = col_name

    def apply(self, row: Dict[str, Any]) -> Any:
        return row.get(self._col_name)
