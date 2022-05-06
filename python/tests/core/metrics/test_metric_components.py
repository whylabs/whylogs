from logging import getLogger

import pytest

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


def test_custom_metric_components() -> None:
    class TestCustomComponent(CustomComponent):
        type_id = 101

    custom_metric_component = TestCustomComponent(12)
    TEST_LOGGER.info(f"metric component is: {custom_metric_component}")
    assert custom_metric_component.value == 12
