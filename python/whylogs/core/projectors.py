from typing import Any, Dict, List


class FieldProjector(object):
    def __init__(self, col_names: List[str]) -> None:
        self._col_names = col_names

    def apply(self, row: Dict[str, Any]) -> Dict[str, Any]:
        row_projection = {col_name: row.get(col_name) for col_name in self._col_names}
        return row_projection
