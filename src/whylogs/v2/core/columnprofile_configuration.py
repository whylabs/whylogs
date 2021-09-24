from dataclasses import dataclass


@dataclass
class ColumnProfileConfiguration:
    strict_column_type: bool = False
    trackers: dict = None
