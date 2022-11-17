from collections import namedtuple
from copy import deepcopy
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Tuple

from whylogs.core.metrics.metrics import Metric
from whylogs.core.utils import deprecated
from whylogs.core.view.column_profile_view import ColumnProfileView
from whylogs.core.view.dataset_profile_view import DatasetProfileView

logger = getLogger(__name__)


ReportResult = namedtuple("ReportResult", "name passed failed summary")


@dataclass
class MetricsSelector:
    metric_name: str
    column_name: Optional[str] = None  # <- TODO: return a collection of columns
    metrics_resolver: Optional[Callable[[DatasetProfileView], List[Metric]]] = None

    def column_profile(self, profile: DatasetProfileView) -> Optional[ColumnProfileView]:
        if profile is None:
            return None
        column_profile_view = profile.get_column(self.column_name)
        if column_profile_view is None:
            logger.warning(
                f"No column name {self.column_name} found in profile, available columns are: {profile.get_columns()}"
            )
        return column_profile_view

    def apply(self, profile: DatasetProfileView) -> List[Metric]:
        if self.metrics_resolver is not None:
            custom_result = self.metrics_resolver(profile)
            if isinstance(custom_result, List):
                return custom_result
            else:
                raise ValueError("metrics_resolver must return a list of Metrics if defined!")
        results: List[Metric] = []
        column_profile_view = self.column_profile(profile)
        if column_profile_view is None:
            return results
        metric = column_profile_view.get_metric(self.metric_name)
        if metric is None:
            logger.warning(
                f"{self.metric_name} not found in {self.column_name} available metrics are: "
                f"{column_profile_view.get_metric_component_paths()}"
            )
        else:
            results.append(metric)
        return results


def pretty_display(metric_name: str, summary: Dict[str, Any]) -> Dict[str, Any]:
    pretty_summary: Dict[str, Any] = {"metric": metric_name}

    for key, item in summary.items():
        if key != "frequent_strings":
            pretty_summary[key] = item
        else:
            pretty_summary[f"{key}_top_10"] = [f"{x.value}:{x.est}" for x in item[:10]]
    return pretty_summary


@dataclass
class MetricConstraint:
    condition: Callable[[Metric], bool]
    name: str
    metric_selector: MetricsSelector
    require_column_existence: bool = True

    def _validate_metrics(self, metrics: List[Metric]) -> bool:
        for metric in metrics:
            if not self.condition(metric):
                return False
        return True

    def _get_metric_summary(self, metrics: List[Metric]) -> Optional[Dict[str, Any]]:
        if len(metrics) == 1:  # Only returns a summary for single metrics.
            metric_summary = pretty_display(metrics[0].namespace, metrics[0].to_summary_dict())
            return metric_summary
        return None

    @deprecated(message="Please use validate_profile()")
    def validate(self, dataset_profile: DatasetProfileView) -> bool:
        # custom metric resolver allows empty metrics
        if self.metric_selector is None:
            raise ValueError("can't call validate with an empty metric selector")
        metric_selector: MetricsSelector = self.metric_selector
        if metric_selector.metrics_resolver is not None:
            metrics = metric_selector.apply(dataset_profile)
            logger.info(f"validate using custom metric selector and found {metrics}")
            return self._validate_metrics(metrics)

        # standard selector requires a column be resolved
        column_profile = metric_selector.column_profile(dataset_profile)
        if column_profile is None:
            if self.require_column_existence:
                raise ValueError(
                    f"Could not get column {metric_selector.column_name}" f"from column {dataset_profile.get_columns()}"
                )
            else:
                logger.info(
                    f"validate could not find column {metric_selector.column_name} "
                    "but require_column_existence is false so returning True"
                )
                return True
        metrics = metric_selector.apply(dataset_profile)
        if metrics is None or len(metrics) == 0:
            if self.require_column_existence:
                logger.info(
                    f"validate could not get metric {metric_selector.metric_name} from column "
                    f"{metric_selector.column_name} so returning False. Available metrics on column are: "
                    f"{column_profile.get_metric_component_paths()}"
                )
                return False
            else:
                logger.info(
                    f"validate could not get metric {metric_selector.metric_name} from column "
                    f"{metric_selector.column_name} but require_column_existence is set so returning True. "
                    f"Available metrics on column are: {column_profile.get_metric_component_paths()}"
                )
                return True
        return self._validate_metrics(metrics)

    def validate_profile(self, dataset_profile: DatasetProfileView) -> Tuple[bool, Optional[Dict[str, Any]]]:
        # custom metric resolver allows empty metrics
        if self.metric_selector is None:
            raise ValueError("can't call validate with an empty metric selector")
        metric_selector: MetricsSelector = self.metric_selector
        if metric_selector.metrics_resolver is not None:
            metrics = metric_selector.apply(dataset_profile)
            logger.info(f"validate using custom metric selector and found {metrics}")
            validate_result = self._validate_metrics(metrics)
            metric_summary = self._get_metric_summary(metrics)
            return (validate_result, metric_summary)

        # standard selector requires a column be resolved
        column_profile = metric_selector.column_profile(dataset_profile)
        if column_profile is None:
            if self.require_column_existence:
                raise ValueError(
                    f"Could not get column {metric_selector.column_name}" f"from column {dataset_profile.get_columns()}"
                )
            else:
                logger.info(
                    f"validate could not find column {metric_selector.column_name} "
                    "but require_column_existence is false so returning True"
                )
                return (True, None)
        metrics = metric_selector.apply(dataset_profile)
        if metrics is None or len(metrics) == 0:
            if self.require_column_existence:
                logger.info(
                    f"validate could not get metric {metric_selector.metric_name} from column "
                    f"{metric_selector.column_name} so returning False. Available metrics on column are: "
                    f"{column_profile.get_metric_component_paths()}"
                )
                return (False, None)
            else:
                logger.info(
                    f"validate could not get metric {metric_selector.metric_name} from column "
                    f"{metric_selector.column_name} but require_column_existence is set so returning True. "
                    f"Available metrics on column are: {column_profile.get_metric_component_paths()}"
                )
                return (True, None)
        validate_result = self._validate_metrics(metrics)
        metric_summary = self._get_metric_summary(metrics)
        return (validate_result, metric_summary)


class Constraints:
    column_constraints: Dict[str, Dict[str, MetricConstraint]]
    dataset_constraints: Dict[str, MetricConstraint]
    dataset_profile_view: Optional[DatasetProfileView]

    def __init__(
        self,
        dataset_profile_view: Optional[DatasetProfileView] = None,
        column_constraints: Optional[Dict[str, Dict[str, MetricConstraint]]] = None,
    ) -> None:
        if column_constraints is None:
            column_constraints = dict()
        self.column_constraints = column_constraints
        self.dataset_constraints = dict()
        self.dataset_profile_view = dataset_profile_view

    def validate(self, profile_view: Optional[DatasetProfileView] = None) -> bool:
        profile = self._resolve_profile_view(profile_view)
        column_names = self.column_constraints.keys()
        if len(column_names) == 0:
            logger.warning("validate was called with empty set of constraints, returning True!")
            return True

        for column_name in column_names:
            columnar_constraints = self.column_constraints[column_name]
            for constraint_name, metric_constraint in columnar_constraints.items():
                (result, _) = metric_constraint.validate_profile(profile)
                if not result:
                    logger.info(f"{constraint_name} failed on column {column_name}")
                    return False
        metric_names = self.dataset_constraints.keys()
        for metric_name in metric_names:
            metric_constraint = self.dataset_constraints[metric_name]
            (result, _) = metric_constraint.validate_profile(profile)
            if not result:
                logger.info(f"{metric_name} failed on dataset")
                return False
        return True

    @deprecated(message="Please use generate_constraints_report()")
    def report(self, profile_view: Optional[DatasetProfileView] = None) -> List[Tuple[str, int, int]]:
        profile = self._resolve_profile_view(profile_view)
        column_names = self.column_constraints.keys()
        results: List[Tuple[str, int, int]] = []
        if len(column_names) == 0:
            logger.warning("report was called with empty set of constraints!")
            return results

        for column_name in column_names:
            columnar_constraints = self.column_constraints[column_name]
            for constraint_name, metric_constraint in columnar_constraints.items():
                (result, _) = metric_constraint.validate_profile(profile)
                if not result:
                    logger.info(f"{constraint_name} failed on column {column_name}")
                    results.append((constraint_name, 0, 1))
                else:
                    results.append((constraint_name, 1, 0))
        metric_names = self.dataset_constraints.keys()
        for metric_name in metric_names:
            metric_constraint = self.dataset_constraints[metric_name]
            (result, _) = metric_constraint.validate_profile(profile)
            if not result:
                logger.info(f"{metric_name} failed on dataset")
                results.append((metric_name, 0, 1))
            else:
                results.append((metric_name, 1, 0))
        return results

    def _generate_metric_report(
        self,
        profile_view: DatasetProfileView,
        metric_constraint: MetricConstraint,
        constraint_name: str,
        with_summary: bool,
    ) -> ReportResult:
        (result, metric_summary) = metric_constraint.validate_profile(profile_view)
        if not result:
            if with_summary:
                return ReportResult(name=constraint_name, passed=0, failed=1, summary=metric_summary)
            else:
                return ReportResult(name=constraint_name, passed=0, failed=1, summary=None)
        else:
            if with_summary:
                return ReportResult(name=constraint_name, passed=1, failed=0, summary=metric_summary)
            else:
                return ReportResult(name=constraint_name, passed=1, failed=0, summary=None)

    def generate_constraints_report(
        self, profile_view: Optional[DatasetProfileView] = None, with_summary=False
    ) -> List[ReportResult]:
        profile = self._resolve_profile_view(profile_view)
        column_names = self.column_constraints.keys()
        results: List[ReportResult] = []
        if len(column_names) == 0:
            logger.warning("report was called with empty set of constraints!")
            return results

        for column_name in column_names:
            columnar_constraints = self.column_constraints[column_name]
            for constraint_name, metric_constraint in columnar_constraints.items():
                metric_report = self._generate_metric_report(
                    profile_view=profile,
                    metric_constraint=metric_constraint,
                    constraint_name=constraint_name,
                    with_summary=with_summary,
                )
                if metric_report[1] == 0:
                    logger.info(f"{constraint_name} failed on column {column_name}")

                results.append(metric_report)

        metric_names = self.dataset_constraints.keys()
        for metric_name in metric_names:
            metric_constraint = self.dataset_constraints[metric_name]
            metric_report = self._generate_metric_report(
                profile_view=profile,
                metric_constraint=metric_constraint,
                constraint_name=metric_name,
                with_summary=with_summary,
            )
            if metric_report[1] == 0:
                logger.info(f"{metric_name} failed on dataset.")

            results.append(metric_report)

        return results

    def _resolve_profile_view(self, profile_view: Optional[DatasetProfileView]) -> DatasetProfileView:
        if profile_view is None and self.dataset_profile_view is None:
            raise ValueError(
                "Constraints need to be initialized on a profile or have a profile passed in before calling validate or report."
            )
        return profile_view if profile_view is not None else self.dataset_profile_view


class ConstraintsBuilder:
    def __init__(self, dataset_profile_view: DatasetProfileView, constraints: Optional[Constraints] = None) -> None:
        self._dataset_profile_view = dataset_profile_view
        if constraints is None:
            constraints = Constraints(dataset_profile_view=dataset_profile_view)
        self._constraints = constraints

    def get_metric_selectors(self) -> List[MetricsSelector]:
        selectors = []
        column_profiles = self._dataset_profile_view.get_columns()
        for column_name in column_profiles:
            column_profile = column_profiles[column_name]
            metric_component_paths = column_profile._metrics.keys()
            for metric_path in metric_component_paths:
                selectors.append(MetricsSelector(metric_name=metric_path, column_name=column_name))
        return selectors

    def add_constraint(self, constraint: MetricConstraint, ignore_missing: bool = False) -> "ConstraintsBuilder":
        # check that the column exists and we have a column profile view
        column_name = constraint.metric_selector.column_name
        column_profile_view = self._dataset_profile_view.get_column(column_name)
        if column_profile_view is None and not ignore_missing and constraint.metric_selector.metrics_resolver is None:
            raise ValueError(
                f"{column_name} was not found in set of this profile, the list of columns are: {self._dataset_profile_view.get_columns()}"
            )

        metric_selector = constraint.metric_selector
        metrics = metric_selector.apply(self._dataset_profile_view)
        if (metrics is None or len(metrics) == 0) and not (ignore_missing or column_name is None):
            raise ValueError(
                f"metrics not found for column {column_name}, available metric components are: {column_profile_view.get_metric_component_paths()}"
            )

        if column_name is None:
            self._constraints.dataset_constraints[constraint.name] = constraint
        else:
            if column_name not in self._constraints.column_constraints:
                self._constraints.column_constraints[column_name] = dict()
            self._constraints.column_constraints[column_name][constraint.name] = constraint

        return self

    def build(self) -> Constraints:
        return deepcopy(self._constraints)
