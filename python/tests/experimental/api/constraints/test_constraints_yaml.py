import pandas as pd

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
import yaml
import pytest
import os
import tempfile

from whylogs.experimental.api.constraints import read_constraints_from_yaml, write_constraints_to_yaml

yaml_string = """\
    
id: "model-23"
version: 1
hash: abcabc231 # maybe from git?

constraints:
  - factory: no_missing_values
    name: blugabluga
    column_name: weight
    metric: counts
  - factory: is_in_range
    column_name: legs
    metric: distribution
    lower: 0
    upper: 4
  - factory: column_is_probably_unique
    column_name: animal
    metric: multi-metric    
    
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


def test_round_trip_constraints_yaml(reference_profile_view):
    with tempfile.TemporaryDirectory() as temp_dir:
        input_yaml_name = "example.yaml"
        output_yaml_name = "example2.yaml"
        # Define the path for the YAML file
        yaml_path = os.path.join(temp_dir, input_yaml_name)

        # Write the YAML string to a file
        with open(yaml_path, "w") as file:
            file.write(yaml_string)
        constraints = read_constraints_from_yaml(os.path.join(temp_dir, input_yaml_name))
        builder = ConstraintsBuilder(dataset_profile_view=reference_profile_view)
        builder.add_constraints(constraints)
        rehydrated_constraints = builder.build()
        write_constraints_to_yaml(
            constraints=rehydrated_constraints, output_path=os.path.join(temp_dir, output_yaml_name)
        )
        constraints_out = read_constraints_from_yaml(os.path.join(temp_dir, output_yaml_name))
        assert len(constraints) == len(constraints_out)
