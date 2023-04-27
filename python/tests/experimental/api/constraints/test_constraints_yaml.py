import os
import tempfile

import pandas as pd
import pytest
import yaml

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
from whylogs.experimental.api.constraints import ConstraintTranslator
from whylogs.experimental.constraints_generation import (
    generate_constraints_from_reference_profile,
)

yaml_string = """\

id: "model-23"
version: 1
hash: abcabc231 # maybe from git?

id: "model-23"
version: 1
hash: abcabc231 # maybe from git?

constraints:
- column_name: weight
  factory: no_missing_values
  metric: counts
  name: customname
- column_name: legs
  factory: is_in_range
  lower: 0
  metric: distribution
  name: legs is in range [0,4]
  upper: 4
- column_name: legs
  factory: condition_meets
  condition_name: legs_even
  name: legs meets condition legs_even
  metric: condition_count
- column_name: animal
  factory: distinct_number_in_range
  lower: 0
  upper: 10
  metric: cardinality
  name: animal distinct values estimate between 0 and 10 (inclusive)
- column_name: legs
  factory: count_below_number
  number: 25
  metric: counts
  name: count of legs lower than 25
- column_name: weight
  factory: is_non_negative
  metric: distribution
  name: weight is non negative
- expression: and <= animal:cardinality/lower_1 :animal:counts/n <= :animal:counts/n
    :animal:cardinality/upper_1
  name: animal is probably unique
  metric: dataset-metric

    """


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


def test_round_trip_constraints_yaml_file(reference_profile_view):
    translator = ConstraintTranslator()
    with tempfile.TemporaryDirectory() as temp_dir:
        input_yaml_name = "example.yaml"
        output_yaml_name = "example2.yaml"
        # Define the path for the YAML file
        yaml_path = os.path.join(temp_dir, input_yaml_name)

        # Write the YAML string to a file
        with open(yaml_path, "w") as file:
            file.write(yaml_string)
        constraints = translator.read_constraints_from_yaml(os.path.join(temp_dir, input_yaml_name))
        builder = ConstraintsBuilder(dataset_profile_view=reference_profile_view)
        builder.add_constraints(constraints)
        rehydrated_constraints = builder.build()
        translator.write_constraints_to_yaml(
            constraints=rehydrated_constraints, output_path=os.path.join(temp_dir, output_yaml_name)
        )
        constraints_out = translator.read_constraints_from_yaml(os.path.join(temp_dir, output_yaml_name))
        assert len(constraints) == len(constraints_out)


def test_round_trip_constraints_yaml_string(reference_profile_view):
    translator = ConstraintTranslator()
    constraints = translator.read_constraints_from_yaml(input_str=yaml_string)
    builder = ConstraintsBuilder(dataset_profile_view=reference_profile_view)
    builder.add_constraints(constraints)
    rehydrated_constraints = builder.build()
    rehydrated_yaml_string = translator.write_constraints_to_yaml(constraints=rehydrated_constraints, output_str=True)
    data1 = yaml.safe_load(yaml_string)
    data2 = yaml.safe_load(rehydrated_yaml_string)
    constraints1 = data1.get("constraints", [])
    constraints2 = data2.get("constraints", [])
    set1 = {frozenset(d.items()) for d in constraints1}
    set2 = {frozenset(d.items()) for d in constraints2}
    assert set1 == set2
