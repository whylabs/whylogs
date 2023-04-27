from typing import Any, Tuple

from whylabs.api.writer.whylabs import WhyLabsWriter
from whylabs.experimental.extras import ConfigHub

from whylogs.api.writer.writer import Writable
from whylogs.core.constraints.metric_constraints import Constraints
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.utils import deprecated_alias
from whylogs.core.view.dataset_profile_view import DatasetProfileView


class CnWriter(WhyLabsWriter):
    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> Tuple[bool, str]:
        if isinstance(file, (DatasetProfile, DatasetProfileView)):
            # check if {org,dataset}_id overridden in kwargs
            constraints_str = ConfigHub(self._org_id, self._dataset_id, "constraints").get_latest()
            constraints = Constraints.deserialize(constraints_str)
            file += constraints.report_columns(file)

        return super().write(file, **kwargs)
