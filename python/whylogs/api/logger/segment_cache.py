import logging
import time
from datetime import datetime
from typing import Dict, Optional

from whylogs.api.logger.result_set import SegmentedResultSet
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.schema import DatasetSchema
from whylogs.core.segment import Segment

logger = logging.getLogger(__name__)


class SegmentCache(object):
    """
    Container for segmented profiles.

    Stores DatasetProfiles while processing inputs using a segmentation DatasetSchema.
    The contained profiles may be stored in memory or on disk.
    """

    def __init__(self, schema: DatasetSchema, segments: Optional[Dict[Segment, DatasetProfile]] = None):
        self._schema = schema
        self._cache = segments or dict()

    def get_or_create_matching_profile(self, segment_key: Segment) -> DatasetProfile:
        profile = self._cache.get(segment_key)
        if profile is None:
            profile = DatasetProfile(schema=self._schema)
            self._cache[segment_key] = profile
        return profile

    def get_segments(self) -> Dict[Segment, DatasetProfile]:
        return self._cache

    def get_result_set(self, dataset_timestamp: Optional[datetime]) -> SegmentedResultSet:
        segmented_profiles: Dict[str, Dict[Segment, DatasetProfile]] = dict()
        for segment_key in self._cache:
            segments = segmented_profiles.get(segment_key.parent_id)
            if segments is None:
                segments = dict()
                segmented_profiles[segment_key.parent_id] = segments
            while self._cache[segment_key].is_active:
                time.sleep(1)
            segments[segment_key] = self._cache[segment_key]

        results = SegmentedResultSet(segments=segmented_profiles, partitions=list(self._schema.segments.values()))
        if dataset_timestamp:
            results.set_dataset_timestamp(dataset_timestamp)
        return results

    def flush(self, dataset_timestamp: Optional[datetime]) -> SegmentedResultSet:
        result_set = self.get_result_set(dataset_timestamp)
        self._cache = dict()
        return result_set
