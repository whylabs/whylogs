import re
from typing import List, Optional, Tuple

from whylogs.core.dataset_profile import DatasetProfile
from whylogs.core.metric_getters import MetricGetter, ProfileGetter
from whylogs.core.metrics.metrics import Metric
from whylogs.core.relations import (
    LiteralGetter,
    Predicate,
    Relation,
    ValueGetter,
    unescape_colon,
    unescape_quote,
)


def _unescape_token(input: str) -> str:
    if input[0] == '"':
        return unescape_quote(input)
    if input[0] == ":":
        return unescape_colon(input)
    return input


# We use this with findall() to split an input string into tokens.
# (?::(?:[^:]|\\:)*:[_a-zA-Z0-9]+/[_a-zA-Z0-9:/]+) matches metric references with \: escaped in column names
# "(?:[^\\"]|\\[^"]|\\")*" matches quoted strings with \"  \\ escaped quotes and \
# (?:[^ ":][^ ]*) matches everything else delimited by spaces

_TOKEN_RE = re.compile(r'(?::(?:[^:]|\\:)*:[_a-zA-Z0-9]+/[_a-zA-Z0-9:/]+)|(?:[^ ":][^ ]*)|"(?:[^\\"]|\\[^"]|\\")*"')


def _tokenize(input: str) -> List[str]:
    return [_unescape_token(t) for t in _TOKEN_RE.findall(input)]


def _get_component(token: List[str], i: int) -> Tuple[str, int]:
    return token[i], i + 1  # either dummy variable x or metric component name in metric::to_summary_dict()


# These match metric references with our without column names, with capture groups to
# pull out the column name and path. They have an optional submetric_name:submetric_namespace/
# section to support MultiMetric references.

_METRIC_REF = re.compile(r"::[_a-zA-Z0-9]+/(?:[_a-zA-Z0-9]+:[_a-zA-Z0-9]+/)?[_a-zA-Z0-9]+")
_PROFILE_REF = re.compile(r":(.+?):([_a-zA-Z0-9]+/(?:[_a-zA-Z0-9]+:[_a-zA-Z0-9]+/)?[_a-zA-Z0-9]+)")


def _get_value(
    token: List[str], i: int, metric: Optional[Metric] = None, profile: Optional[DatasetProfile] = None
) -> Tuple[ValueGetter, int]:
    if token[i].startswith('"'):
        return LiteralGetter(token[i][1:-1]), i + 1

    if _METRIC_REF.fullmatch(token[i]):
        if metric is None:
            raise ValueError("Must specify metric to use with MetricGetter")

        namespace, path = token[i][2:].split("/", 1)
        if metric.namespace != namespace:
            raise ValueError(f"Expected {namespace} metric but got {metric.namespace}")

        return MetricGetter(metric, path), i + 1

    match = _PROFILE_REF.fullmatch(token[i])
    if bool(match):
        if profile is None:
            raise ValueError("Must specify profile to use with ProfileGetter")

        column_name, path = match.groups()  # type: ignore
        return ProfileGetter(profile, column_name, path), i + 1

    if bool(re.fullmatch(r"[-+]?\d+", token[i])):
        return LiteralGetter(int(token[i])), i + 1

    return LiteralGetter(float(token[i])), i + 1


def _deserialize(
    token: List[str], i: int, metric: Optional[Metric] = None, profile: Optional[DatasetProfile] = None
) -> Tuple[Predicate, int]:
    if token[i] == "~":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.match, value(), component=component), i

    if token[i] == "~=":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.fullmatch, value(), component=component), i

    if token[i] == "==":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.equal, value, component=component), i

    if token[i] == "<":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.less, value, component=component), i

    if token[i] == "<=":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.leq, value, component=component), i

    if token[i] == ">":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.greater, value, component=component), i

    if token[i] == ">=":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.geq, value, component=component), i

    if token[i] == "!=":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.neq, value, component=component), i

    if token[i] == "and":
        left, i = _deserialize(token, i + 1)
        right, i = _deserialize(token, i, metric, profile)
        return Predicate(Relation._and, left=left, right=right, component=left._component), i

    if token[i] == "or":
        left, i = _deserialize(token, i + 1)
        right, i = _deserialize(token, i, metric, profile)
        return Predicate(Relation._or, left=left, right=right, component=left._component), i

    if token[i] == "not":
        right, i = _deserialize(token, i + 1)
        return Predicate(Relation._not, right=right, component=right._component), i

    if token[i] == "%":
        component, i = _get_component(token, i + 1)
        value, i = _get_value(token, i, metric, profile)
        return Predicate(Relation.search, value(), component=component), i

    raise ValueError("Unable to parse predicate expression '{' '.join(token)}' at token {i+1}.")


def parse_predicate(
    expression: str, metric: Optional[Metric] = None, profile: Optional[DatasetProfile] = None
) -> Predicate:
    predicate, _ = _deserialize(_tokenize(expression), 0, metric, profile)
    return predicate
