from dataclasses import dataclass
from typing import Optional
from datetime import datetime


@dataclass
class EstimationResult:
    """
    The result of a performance estimation.
    accuracy: The estimated accuracy.
    reference_partition_id: The partition id of the reference result set.
    reference_result_timestamp: The timestamp of the reference result set.
    """

    accuracy: Optional[float] = None
    reference_partition_id: Optional[str] = None
    reference_result_timestamp: Optional[datetime] = None
    target_result_timestamp: Optional[datetime] = None
