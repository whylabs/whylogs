from copy import deepcopy
from logging import getLogger

import pytest
import whylogs_sketching as ds  # type: ignore

from whylogs.core.metrics.metric_components import (
    CustomComponent,
    FractionalComponent,
    FrequentStringsComponent,
    HllComponent,
    IntegralComponent,
    KllComponent,
    MaxIntegralComponent,
    MinIntegralComponent,
)

TEST_LOGGER = getLogger(__name__)
_TEST_COMPONENT_TYPES = [
    IntegralComponent,
    MinIntegralComponent,
    MaxIntegralComponent,
    FractionalComponent,
    KllComponent,
    HllComponent,
    FrequentStringsComponent,
]


@pytest.mark.parametrize("component_type", _TEST_COMPONENT_TYPES)
def test_metric_components(component_type) -> None:
    metric_component = component_type(12)
    TEST_LOGGER.info(f"metric component is: {metric_component}")
    TEST_LOGGER.info(f"{dir(metric_component)}")
    assert metric_component.value == 12


def test_min_component() -> None:
    first = MinIntegralComponent(1)
    second = MinIntegralComponent(2)
    res = first + second
    assert res.value == 1


def test_max_component() -> None:
    first = MaxIntegralComponent(1)
    second = MaxIntegralComponent(2)
    res = first + second
    assert res.value == 2


def test_component_deepcopy() -> None:
    orig = FrequentStringsComponent(ds.frequent_strings_sketch(lg_max_k=10))
    copy1 = deepcopy(orig)
    copy2 = deepcopy(copy1)  # noqa  the test passes if this line doesn't crash


def test_custom_metric_components() -> None:
    class TestCustomComponent(CustomComponent):
        type_id = 101

    custom_metric_component = TestCustomComponent(12)
    TEST_LOGGER.info(f"metric component is: {custom_metric_component}")
    assert custom_metric_component.value == 12
