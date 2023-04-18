import pandas as pd
import pytest

import whylogs as why
from whylogs.core.constraints import ConstraintsBuilder
from whylogs.core.metrics.condition_count_metric import (
    Condition,
    ConditionCountConfig,
    ConditionCountMetric,
)
from whylogs.core.relations import Predicate
from whylogs.core.resolvers import STANDARD_RESOLVER, MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.experimental.constraints_generation import (
    generate_constraints_from_reference_profile,
)


@pytest.fixture
def reference_profile_view():
    data = {
        "animal": ["cat", "hawk", "snake", "cat", "mosquito"],
        "legs": [4, 2, 0, 4, 6],
        "weight": [4.3, 1.8, 1.3, 4.1, 5.5e-6],
    }
    df = pd.DataFrame(data)

    def even(x) -> bool:
        return x % 2 == 0

    def odd(x) -> bool:
        return x % 2 != 0

    legs_conditions = {"legs_even": Condition(Predicate().is_(even)), "legs_odd": Condition(Predicate().is_(odd))}

    legs_spec = ResolverSpec(
        column_name="legs",
        metrics=[
            MetricSpec(
                ConditionCountMetric,
                ConditionCountConfig(conditions=legs_conditions),
            ),
        ],
    )
    schema = DeclarativeSchema(STANDARD_RESOLVER + [legs_spec])

    results = why.log(df, schema=schema)
    profile_view = results.view()
    return profile_view


@pytest.fixture
def target_profile_view():
    data = {
        "animal": ["cat", "hawk", "snake", "cat", "mosquito"],
        "legs": [4, 2, 0, 4, 6],
        "weight": [4.3, "a", 1.3, 4.1, 5.5e-6],
    }
    df = pd.DataFrame(data)

    def even(x) -> bool:
        return x % 2 == 0

    def odd(x) -> bool:
        return x % 2 != 0

    legs_conditions = {"legs_even": Condition(Predicate().is_(even)), "legs_odd": Condition(Predicate().is_(odd))}

    legs_spec = ResolverSpec(
        column_name="legs",
        metrics=[
            MetricSpec(
                ConditionCountMetric,
                ConditionCountConfig(conditions=legs_conditions),
            ),
        ],
    )
    schema = DeclarativeSchema(STANDARD_RESOLVER + [legs_spec])

    results = why.log(df, schema=schema)
    profile_view = results.view()
    return profile_view


def test_constraints_generation(reference_profile_view, target_profile_view):
    constraints_assertions = [
        ("animal has no missing values", 1),
        ("animal allows for types ['string']", 1),
        ("legs has no missing values", 1),
        ("legs allows for types ['integral']", 1),
        ("legs meets condition legs_even", 1),
        ("legs never meets condition legs_odd", 1),
        ("legs is non negative", 1),
        ("weight has no missing values", 1),
        ("weight allows for types ['fractional']", 0),
        ("weight is non negative", 1),
        ("weight is probably unique", 1),
    ]

    suggested_constraints = generate_constraints_from_reference_profile(reference_profile_view=reference_profile_view)
    builder = ConstraintsBuilder(dataset_profile_view=target_profile_view)
    builder.add_constraints(suggested_constraints)
    constraints = builder.build()
    report = constraints.generate_constraints_report()
    for constraint_name, passed in constraints_assertions:
        constraint = next((c for c in report if c.name == constraint_name), None)
        assert constraint and constraint.passed == passed


def test_items_in_set_constraint():
    data = {"animal": ["cat", "hawk"] * 1000}
    target_data = {"animal": ["snake", "mosquito"] * 1000}

    ref_df = pd.DataFrame(data)
    target_df = pd.DataFrame(target_data)
    ref = why.log(ref_df).view()
    target = why.log(target_df).view()
    suggested_constraints = generate_constraints_from_reference_profile(reference_profile_view=ref)
    builder = ConstraintsBuilder(dataset_profile_view=target)
    builder.add_constraints(suggested_constraints)
    constraints = builder.build()
    report = constraints.generate_constraints_report()
    constraint = next((c for c in report if c.name == "animal values in set {'cat', 'hawk'}"), None)
    assert constraint and constraint.passed == 0


def test_inclusion_and_exclusion_lists(reference_profile_view, target_profile_view):
    suggested_constraints = generate_constraints_from_reference_profile(
        reference_profile_view=reference_profile_view, included_columns=["animal"]
    )
    builder = ConstraintsBuilder(dataset_profile_view=target_profile_view)
    builder.add_constraints(suggested_constraints)
    constraints = builder.build()
    report = constraints.generate_constraints_report()
    assert all(["animal" in c.name for c in report])

    suggested_constraints = generate_constraints_from_reference_profile(
        reference_profile_view=reference_profile_view, excluded_columns=["legs", "weight"]
    )
    builder = ConstraintsBuilder(dataset_profile_view=target_profile_view)
    builder.add_constraints(suggested_constraints)
    constraints = builder.build()
    report = constraints.generate_constraints_report()
    assert all(["animal" in c.name for c in report])
