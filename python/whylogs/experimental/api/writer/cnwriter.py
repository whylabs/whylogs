from whylabs.api.writer.whylabs import WhyLabsWriter
from whylabs.experimental.extras import ConfigHub

from whylogs.core.utils import deprecated_alias
from typing import Tuple
from whylogs.api.writer.writer import Writable
from whylogs.core.constraints.metric_constraints import Constraints

class CnWriter(WhyLabsWriter):

    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> Tuple[bool, str]:
        if isinstance(file, (DatasetProfile, DatasetProfileView)):
            # check if {org,dataset}_id overridden in kwargs
            constraints_str = ConfigHub(self._org_id, self._dataset_id, "constraints").get_latest()
            constraints = Constraints.deserialize(constraints_str)
            file += constraints.report_columns(file)

        return super().write(file, **kwargs)
