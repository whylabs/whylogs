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

    legs_conditions = {"legs_even": Condition(Predicate().is_(even))}

    legs_spec = ResolverSpec(
        column_name="legs",
        metrics=[
            MetricSpec(
                ConditionCountMetric,
                ConditionCountConfig(conditions=legs_conditions),
            ),
        ],
    )
    schema = DeclarativeSchema(STANDARD_RESOLVER)
    schema.add_resolver(legs_spec)

    results = why.log(df, schema=schema)
    profile_view = results.view()
    return profile_view


def test_constraints_generation(reference_profile_view):
    suggested_constraints = generate_constraints_from_reference_profile(reference_profile_view=reference_profile_view)
    # since the profile is the reference itself, every condition should pass
    builder = ConstraintsBuilder(dataset_profile_view=reference_profile_view)
    builder.add_constraints(suggested_constraints)
    constraints = builder.build()
    report = constraints.generate_constraints_report()
    assert all(result.passed == 1 for result in report)
    assert report[0].name == "animal has no missing values"
    assert report[1].name == "animal types count non-zero for ['string']"
    assert report[2].name == "legs has no missing values"
    assert report[3].name == "legs types count non-zero for ['integral']"
    assert report[4].name == "legs meets condition legs_even"
    assert report[5].name == "weight has no missing values"
    assert report[6].name == "weight types count non-zero for ['fractional']"
