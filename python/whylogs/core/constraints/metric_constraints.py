import re
from collections import namedtuple
from copy import deepcopy
from dataclasses import dataclass
from logging import getLogger
from typing import Any, Callable, Dict, List, Optional, Tuple, Union

from whylogs.core.configs import SummaryConfig
from whylogs.core.metrics.metrics import Metric
from whylogs.core.predicate_parser import _METRIC_REF, _PROFILE_REF, _tokenize
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

    def column_profile(self, profile: Optional[DatasetProfileView]) -> Optional[ColumnProfileView]:
        if profile is None:
            return None
        column_profile_view = profile.get_column(self.column_name)
        if self.column_name and column_profile_view is None:
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
                    f"{metric_selector.column_name} but require_column_existence is not set so returning True. "
                    f"Available metrics on column are: {column_profile.get_metric_component_paths()}"
                )
                return (True, None)
        if len(metrics) == 1 and metrics[0].namespace == "distribution":
            if metrics[0].kll.value.is_empty() and self.require_column_existence:
                logger.info(
                    f"validate reached empty distribution metrics from column "
                    f"{metric_selector.column_name} but require_column_existence is set so returning False. "
                    f"Available metrics on column are: {column_profile.get_metric_component_paths()}"
                )
                return (False, None)
            if metrics[0].kll.value.is_empty() and not self.require_column_existence:
                logger.info(
                    f"validate reached empty distribution metrics from column "
                    f"{metric_selector.column_name} but require_column_existence is not set so returning True. "
                    f"Available metrics on column are: {column_profile.get_metric_component_paths()}"
                )
                return (True, None)

        validate_result = self._validate_metrics(metrics)
        metric_summary = self._get_metric_summary(metrics)
        return (validate_result, metric_summary)


class MissingMetric(Exception):
    """
    DatasetConstraint conditions can raise this exception to indicate that
    a required metric or column could not be found in the DatasetProfileView.
    The DatasetConstraint is responsible for handling this exception.
    """


@dataclass(frozen=True)
class DatasetComparisonConstraint:
    """
    Implements dataset-level constraints that require a reference dataset to
    be compared against. The condition Callable takes the DatasetProfileView
    of the dataset to be validated as well as the DatasetProfileView of the
    reference dataset and returns a boolean indicating whether the condition
    is satisfied as well as a dictionary with summary information about the
    validation.

    """

    condition: Callable[[DatasetProfileView, DatasetProfileView], Tuple[bool, Dict[str, Metric]]]
    name: str

    def validate_profile(
        self, dataset_profile: DatasetProfileView, reference_profile: DatasetProfileView
    ) -> Tuple[bool, Optional[Dict[str, Any]]]:
        validate_result, summary = self.condition(dataset_profile, reference_profile)
        return (validate_result, summary)


@dataclass(frozen=True)
class DatasetConstraint:
    """
    Implements dataset-level constraints that are not attached to a specific
    metric or column. The condition Callable takes the DatasetProfileView as
    input and returns a boolean indicating whether the condition is satisfied
    as well as a dictionary mapping 'column_name/metric_namespace' -> Metric for
    any metrics the condition used during its evaluation.
    """

    condition: Callable[[DatasetProfileView], Tuple[bool, Dict[str, Metric]]]
    name: str
    require_column_existence: bool = True  # Applies to all columns referenced in the constraint

    def _get_metric_summary(self, metrics: Dict[str, Metric]) -> Optional[Dict[str, Any]]:
        summary: Dict[str, Any] = dict()
        for path, metric in metrics.items():
            metric_summary = pretty_display(metric.namespace, metric.to_summary_dict())
            for component, value in metric_summary.items():
                summary[f"{path}/{component}"] = value
        return summary

    def validate_profile(self, dataset_profile: DatasetProfileView) -> Tuple[bool, Optional[Dict[str, Any]]]:
        try:
            validate_result, metrics = self.condition(dataset_profile)
        except MissingMetric as e:
            if self.require_column_existence:
                logger.info(f"validate_profile could not get metric {str(e)} so returning False.")
                return (False, None)

            logger.info(
                f"validate_profile could not get metric {str(e)}  "
                f"but require_column_existence is False, so returning True. "
            )
            return (True, None)

        metric_summary = self._get_metric_summary(metrics)
        return (validate_result, metric_summary)


class MetricConstraintWrapper:
    """
    Converts a MetricConstraint not associated with a specific column
    into a Callable that can be a DatasetConstraint condition.
    """

    def __init__(self, constraint: MetricConstraint) -> None:
        self.constraint = constraint

    def __call__(self, profile: DatasetProfileView) -> Tuple[bool, Dict[str, Metric]]:
        valid, _ = self.constraint.validate_profile(profile)
        metrics = self.constraint.metric_selector.apply(profile)  # Metrics used during evaluation
        columns = profile.get_columns()
        metric_map: Dict[str, Metric] = dict()
        # Find the columns the referenced Metrics live in
        for metric in metrics:
            for column_name, column_view in columns.items():
                if metric in column_view.get_metrics():
                    metric_map[f"{column_name}/{metric.namespace}"] = metric
                    break
        return valid, metric_map


_INT_RE = re.compile(r"[-+]?\d+")
_FLOAT_RE = re.compile(r"[-+]?(\d+(\.\d*)?|\.\d+)([eE][-+]?\d+)?")


class PrefixCondition:
    """
    Interpret expressions in the form used to serialize Predicate expressions.
    This is probably a reasonable serialization format for DatasetConstraint
    conditions, but might not be the best user interface for creating conditions
    in client code.
    """

    def __init__(self, expression: str, cfg: Optional[SummaryConfig] = None) -> None:
        self._expression = expression
        self._tokens = _tokenize(expression)
        self._profile = None
        self._metric_map: Dict[str, Metric] = dict()
        self._cfg = cfg or SummaryConfig()

    def _interpret(self, i: int) -> Tuple[Any, int]:  # noqa: C901
        token = self._tokens[i]

        # Metric reference        :column_name:metric_namespace/component_name
        # MultiMetric reference   :column_name:metric_namespace/submetric_name:submetric_namespace/component_name
        # Dataset-level metric    ::metric_namespace/...

        match = _METRIC_REF.fullmatch(token)
        if bool(match):
            path = token[2:]
            # TODO: get dataset-level metric from profile
            # TODO: self._metric_map[path] = metric
            return 0, i + 1

        match = _PROFILE_REF.fullmatch(token)
        if bool(match):
            column_name, path = match.groups()
            metric_name, component_name = path.split("/", 1)
            try:
                metric = self._profile.get_column(column_name).get_metric(metric_name)  # type: ignore
                summary = metric.to_summary_dict(self._cfg)
            except:  # noqa
                raise MissingMetric(token)

            # Track Metrics referenced during evaluation
            metric_path = f"{column_name}/{metric_name}"
            self._metric_map[metric_path] = metric
            try:
                value = summary[component_name]
            except:  # noqa
                raise ValueError(f"Component {component_name} not found in {metric_path}")

            return value, i + 1

        # Relational operators

        if token == "~":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            regex = re.compile(right) if isinstance(right, str) else None
            # TODO: should we try to str(left) ?
            return (regex and isinstance(left, str) and bool(regex.match(left))), i

        if token == "~=":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            regex = re.compile(right) if isinstance(right, str) else None
            # TODO: should we try to str(left) ?
            return (regex and isinstance(left, str) and bool(regex.fullmatch(left))), i

        if token == "==":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left == right), i

        if token == "!=":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left != right), i

        if token == "<":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left < right), i

        if token == "<=":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left <= right), i

        if token == ">":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left > right), i

        if token == ">=":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left >= right), i

        # Boolean operators

        if token == "and":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left and right), i

        if token == "or":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left or right), i

        if token == "not":
            right, i = self._interpret(i + 1)
            return (not right), i

        # Arithmetic operators

        if token == "+":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left + right), i

        if token == "-":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left - right), i

        if token == "*":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left * right), i

        if token == "**":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left**right), i

        if token == "/":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left / right), i

        if token == "//":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left // right), i

        if token == "%":
            left, i = self._interpret(i + 1)
            right, i = self._interpret(i)
            return (left % right), i

        # Literals

        if token.startswith('"'):
            return token[1:-1], i + 1

        if bool(_INT_RE.fullmatch(token)):
            return int(token), i + 1

        if bool(_FLOAT_RE.fullmatch(token)):
            return float(token), i + 1

        raise ValueError(f"Unparsable expression: '{self._expression}' at token {i}: '{token}'")

    def __call__(self, profile: DatasetProfileView) -> Tuple[bool, Optional[Dict[str, Metric]]]:
        """
        relational operators: ~ ~= == < <= > >= !=
        boolean operators:    and or not
        arithmetic operators: + - * ** / // %

        literals: 42 3.14 "blah"

        metric component references: :column_name:metric_namespace/metric_component

        returns a bool indicating whether the expression passed and a map of
        :column_name:metric_namespace -> Metric for the metrics used in evaluating
        the expression.
        """
        self._profile = profile
        self._metric_map = dict()
        passes, _ = self._interpret(0)
        if not isinstance(passes, bool):
            raise ValueError(f"Contraint expression should return a boolean, got {type(passes)}")

        return passes, self._metric_map


def _make_report(
    constraint_name: str, result: bool, with_summary: bool, metric_summary: Optional[Dict[str, Any]]
) -> ReportResult:
    if not result:
        if with_summary:
            return ReportResult(name=constraint_name, passed=0, failed=1, summary=metric_summary)
        return ReportResult(name=constraint_name, passed=0, failed=1, summary=None)

    if with_summary:
        return ReportResult(name=constraint_name, passed=1, failed=0, summary=metric_summary)
    return ReportResult(name=constraint_name, passed=1, failed=0, summary=None)


class Constraints:
    column_constraints: Dict[str, Dict[str, MetricConstraint]]
    dataset_constraints: List[DatasetConstraint]
    dataset_comparison_constraints: List[DatasetComparisonConstraint]
    dataset_profile_view: Optional[DatasetProfileView]

    def __init__(
        self,
        dataset_profile_view: Optional[DatasetProfileView] = None,
        column_constraints: Optional[Dict[str, Dict[str, MetricConstraint]]] = None,
        reference_profile_view: Optional[DatasetProfileView] = None,
    ) -> None:
        self.column_constraints = column_constraints or dict()
        self.dataset_constraints = []
        self.dataset_comparison_constraints = []
        self.dataset_profile_view = dataset_profile_view
        self.reference_profile_view = reference_profile_view

    def validate(self, profile_view: Optional[DatasetProfileView] = None) -> bool:
        profile = self._resolve_profile_view(profile_view)
        column_names = self.column_constraints.keys()
        if (
            len(column_names) == 0
            and len(self.dataset_constraints) == 0
            and len(self.dataset_comparison_constraints) == 0
        ):
            logger.warning("validate was called with empty set of constraints, returning True!")
            return True

        for column_name in column_names:
            columnar_constraints = self.column_constraints[column_name]
            for constraint_name, metric_constraint in columnar_constraints.items():
                (result, _) = metric_constraint.validate_profile(profile)
                if not result:
                    logger.info(f"{constraint_name} failed on column {column_name}")
                    return False

        for constraint in self.dataset_constraints + self.dataset_comparison_constraints:
            if isinstance(constraint, DatasetConstraint):
                (result, _) = constraint.validate_profile(profile)
            elif isinstance(constraint, DatasetComparisonConstraint):
                (result, _) = constraint.validate_profile(profile, self.reference_profile_view)
            if not result:
                logger.info(f"{constraint.name} failed on dataset")
                return False
        return True

    def set_reference_profile_view(self, profile_view: DatasetProfileView) -> None:
        self.reference_profile_view = profile_view

    @deprecated(message="Please use generate_constraints_report()")
    def report(self, profile_view: Optional[DatasetProfileView] = None) -> List[Tuple[str, int, int]]:
        profile = self._resolve_profile_view(profile_view)
        column_names = self.column_constraints.keys()
        results: List[Tuple[str, int, int]] = []
        for column_name in column_names:
            columnar_constraints = self.column_constraints[column_name]
            for constraint_name, metric_constraint in columnar_constraints.items():
                (result, _) = metric_constraint.validate_profile(profile)
                if not result:
                    logger.info(f"{constraint_name} failed on column {column_name}")
                    results.append((constraint_name, 0, 1))
                else:
                    results.append((constraint_name, 1, 0))
        for constraint in self.dataset_constraints:
            result, _ = constraint.validate_profile(profile)
            if not result:
                logger.info(f"{constraint.name} failed on dataset")
                results.append((constraint.name, 0, 1))
            else:
                results.append((constraint.name, 1, 0))

        if len(results) == 0:
            logger.warning("report was called with empty set of constraints!")

        return results

    def _generate_metric_report(
        self,
        profile_view: DatasetProfileView,
        metric_constraint: MetricConstraint,
        constraint_name: str,
        with_summary: bool,
    ) -> ReportResult:
        (result, metric_summary) = metric_constraint.validate_profile(profile_view)
        return _make_report(constraint_name, result, with_summary, metric_summary)

    def _generate_dataset_report(
        self,
        profile_view: DatasetProfileView,
        constraint: Union[DatasetConstraint, DatasetComparisonConstraint],
        with_summary: bool,
    ) -> ReportResult:
        if isinstance(constraint, DatasetComparisonConstraint):
            (result, summary) = constraint.validate_profile(profile_view, self.reference_profile_view)
        elif isinstance(constraint, DatasetConstraint):
            (result, summary) = constraint.validate_profile(profile_view)
        return _make_report(constraint.name, result, with_summary, summary)

    def generate_constraints_report(
        self, profile_view: Optional[DatasetProfileView] = None, with_summary=False
    ) -> List[ReportResult]:
        profile = self._resolve_profile_view(profile_view)
        column_names = self.column_constraints.keys()
        results: List[ReportResult] = []

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

        for constraint in self.dataset_constraints + self.dataset_comparison_constraints:
            metric_report = self._generate_dataset_report(
                profile_view=profile,
                constraint=constraint,
                with_summary=with_summary,
            )
            if metric_report[1] == 0:
                logger.info(f"{constraint.name} failed on dataset.")

            results.append(metric_report)

        if len(results) == 0:
            logger.warning("generate_constraints_report was called with empty set of constraints!")

        return results

    def _resolve_profile_view(self, profile_view: Optional[DatasetProfileView]) -> DatasetProfileView:
        if profile_view is None and self.dataset_profile_view is None:
            raise ValueError(
                "Constraints need to be initialized on a profile or have a profile passed in before calling validate or report."
            )
        return profile_view if profile_view is not None else self.dataset_profile_view


class ConstraintsBuilder:
    def __init__(
        self,
        dataset_profile_view: DatasetProfileView,
        constraints: Optional[Constraints] = None,
        reference_profile_view: Optional[DatasetProfileView] = None,
    ) -> None:
        self._dataset_profile_view = dataset_profile_view
        if constraints is None:
            constraints = Constraints(dataset_profile_view=dataset_profile_view)
        self._constraints = constraints
        self._reference_profile_view = reference_profile_view

    def get_metric_selectors(self) -> List[MetricsSelector]:
        selectors = []
        column_profiles = self._dataset_profile_view.get_columns()
        for column_name, column_profile in column_profiles.items():
            metric_component_paths = column_profile._metrics.keys()
            for metric_path in metric_component_paths:
                selectors.append(MetricsSelector(metric_name=metric_path, column_name=column_name))
        return selectors

    def add_constraints(self, constraints: List[Union[MetricConstraint, DatasetConstraint]]) -> "ConstraintsBuilder":
        for constraint in constraints:
            self.add_constraint(constraint)
        return self

    def add_constraint(
        self, constraint: Union[MetricConstraint, DatasetConstraint], ignore_missing: bool = False
    ) -> "ConstraintsBuilder":
        if isinstance(constraint, MetricConstraint):
            # check that the column exists and we have a column profile view
            column_name = constraint.metric_selector.column_name
            column_profile_view = self._dataset_profile_view.get_column(column_name)
            if (
                column_name is not None
                and column_profile_view is None
                and not ignore_missing
                and constraint.metric_selector.metrics_resolver is None
            ):
                raise ValueError(
                    f"Column '{column_name}' was not found in this profile, the available columns are: {self._dataset_profile_view.get_columns()}"
                )

            metric_selector = constraint.metric_selector
            metrics = metric_selector.apply(self._dataset_profile_view)
            if (metrics is None or len(metrics) == 0) and not (ignore_missing or column_name is None):
                logger.warning(
                    f"metrics not found for {column_name}, available metric components are: {column_profile_view.get_metric_component_paths()}. Skipping {constraint.name}."  # noqa: E501
                )
                return self

            if column_name is None:
                # MetricConstraint not associated with a specific column is turned into
                # a DatasetConstraint. It relies on the constraint's metric_selector
                # to find the metrics it needs to evaluate the condition.
                self._constraints.dataset_constraints.append(
                    DatasetConstraint(
                        condition=MetricConstraintWrapper(constraint),
                        name=constraint.name,
                        require_column_existence=constraint.require_column_existence,
                    )
                )
            else:
                if column_name not in self._constraints.column_constraints:
                    self._constraints.column_constraints[column_name] = dict()
                self._constraints.column_constraints[column_name][constraint.name] = constraint

        if isinstance(constraint, DatasetConstraint):
            self._constraints.dataset_constraints.append(constraint)
        if isinstance(constraint, DatasetComparisonConstraint):
            if not self._reference_profile_view:
                raise ValueError(
                    "ConstraintsBuilder: reference_profile_view must be set when adding DatasetComparisonConstraint"
                )
            self._constraints.set_reference_profile_view(self._reference_profile_view)
            self._constraints.dataset_comparison_constraints.append(constraint)

        return self

    def build(self) -> Constraints:
        return deepcopy(self._constraints)
