from typing import Any, Dict, List, Optional, Tuple

from whylabs.api.writer.whylabs import WhyLabsWriter
from whylabs.experimental.extras import ConfigStore
from whylabs_client import ApiClient

import whylogs as why

from whylogs.api.writer.writer import Writable
from whylogs.core.constraints import ConstraintsBuilder
from whylogs.core.constraints.metric_constraints import ReportResult
from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.utils import deprecated_alias
from whylogs.core.view.dataset_profile_view import DatasetProfileView
from whylogs.experimental.api.constraints import ConstraintTranslator


# TODO: maybe move these to whylogs/migrate/uncompound.py ?

def _uncompounded_column_prefix() -> str:
    return "Ω.whylabs"

def _uncompound_report(reports: List[ReportResult]) -> Dict[str, int]:
    result: Dict[str, int] = dict()
    for report in reports:
        if report.column_name is not None:
            name_componenets = [
                _uncompounded_column_prefix(),
                "column_constraint",
                report.column_name,
                report.name,
                "failed",
            ]
        else:
            name_components = [
                _uncompounded_column_prefix(),
                "dataset_constraint",
                report.name,
                "failed",
            ]
        uncompounded_name = ".".join(name_components)
        result[uncomponded_name] = report.failed
    return result


class CnWriter(WhyLabsWriter):
    """
    This is WhyLabsWriter that runs any constraint suite for the profile
    and adds the report as columns that can be monitored in WhyLabs
    """

    def __init__(
        self,
        org_id: Optional[str] = None,
        api_key: Optional[str] = None,
        dataset_id: Optional[str] = None,
        api_client: Optional[ApiClient] = None,
        ssl_ca_cert: Optional[str] = None,
        _timeout_seconds: Optional[float] = None,
        constraint_store: Optional[ConfigStore] = None,
    ):
        if constraint_store is not None and not isinstance(constraint_store, ConfigStore):
            raise ValueError("constraint_store must be a subclass of ConfigStore")

        self._cn_store = constraint_store
        super().__init__(org_id, api_key, dataset_id, api_client, ssl_ca_cert, _timeout_seconds)

    @deprecated_alias(profile="file")
    def write(self, file: Writable, **kwargs: Any) -> Tuple[bool, str]:
        view = file.view() if isinstance(file, DatasetProfile) else file
        if (
            isinstance(file, DatasetProfileView)
            and self._cn_store is not None
            and self._cn_store.exists()
        ):
            # turn constraint report into columns and append them to the profile
            constraints_str = self._cn_store.get_latest()
            constraints = ConstraintTranslator().read_constraints_from_yaml(input_str=constraints_str)
            builder = ConstraintsBuilder(file)
            builder.add_constraints(constraints)
            report = builder.build().generate_constraints_report()
            uncompounded_report = _uncompound_report(report)
            report_view = why.log(row=uncompounded_report).profile().view()
            file._columns.update(report_view._columns)

        return super().write(file, **kwargs)
