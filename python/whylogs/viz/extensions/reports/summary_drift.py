import logging
from typing import Dict, List, Optional

import whylogs.viz.drift.column_drift_algorithms as column_drift_algorithms
from whylogs import DatasetProfileView
from whylogs.viz.enums.enums import PageSpecEnum
from whylogs.viz.extensions.reports.html_report import HTMLReport
from whylogs.viz.utils.html_template_utils import get_compiled_template
from whylogs.viz.utils.profile_viz_calculations import (
    generate_summaries_with_drift_score,
)

logger = logging.getLogger(__name__)


class SummaryDriftReport(HTMLReport):
    _drift_map: Optional[Dict[str, column_drift_algorithms.ColumnDriftAlgorithm]] = None

    def __init__(self, ref_view: DatasetProfileView, target_view: DatasetProfileView, height: Optional[str] = None):
        super().__init__(ref_view=ref_view, target_view=target_view, height=height)

    def report(self) -> str:
        page_spec = PageSpecEnum.SUMMARY_REPORT.value
        template = get_compiled_template(page_spec.html)

        profiles_summary = generate_summaries_with_drift_score(
            self.target_view, self.ref_view, config=None, drift_map=self._drift_map
        )
        rendered_template = template(profiles_summary)
        summary_drift_report = self.display(rendered_template, page_spec)
        return summary_drift_report

    def add_drift_config(
        self, column_names: List[str], algorithm: column_drift_algorithms.ColumnDriftAlgorithm
    ) -> None:
        """Add drift configuration.
        The algorithms and thresholds added through this method will be used to calculate drift scores in the `summary_drift_report()` method.
        If any drift configuration exists, the new configuration will overwrite the standard behavior when appliable.
        If a column has multiple configurations defined, the last one defined will be used.

        Parameters
        ----------
        config: DriftConfig, required
            Drift configuration.

        """
        self._drift_map = {} if not self._drift_map else self._drift_map
        if not isinstance(algorithm, column_drift_algorithms.ColumnDriftAlgorithm):
            raise ValueError("Algorithm must be of class ColumnDriftAlgorithm.")
        if not self.target_view or not self.ref_view:
            logger.error("Set target and reference profiles before adding drift configuration.")
            raise ValueError
        if not algorithm:
            raise ValueError("Drift algorithm cannot be None.")
        if not column_names:
            raise ValueError("Drift configuration must have at least one column name.")
        if column_names:
            for column_name in column_names:
                if column_name not in self.target_view.get_columns().keys():
                    raise ValueError(f"Column {column_name} not found in target profile.")
                if column_name not in self.target_view.get_columns().keys():
                    raise ValueError(f"Column {column_name} not found in reference profile.")
        for column_name in column_names:
            if column_name in self._drift_map:
                logger.warning(f"Overwriting existing drift configuration for column {column_name}.")
            self._drift_map[column_name] = algorithm
