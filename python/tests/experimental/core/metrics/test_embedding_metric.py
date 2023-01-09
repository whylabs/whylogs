
import logging

import numpy as np

import whylogs as why
from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema

from whylogs.experimental.core.metrics.embedding_metric import EmbeddingConfig, EmbeddingMetric


def test_embedding_metric_holds_the_smoke_in() -> None:
    config = EmbeddingConfig(
        references = np.array([[0, 1], [0, 1], [0,1]]),
        labels = ["A", "B"]
    )
    schema = DeclarativeSchema([
        ResolverSpec(
            column_name = "col1",
            metrics = [MetricSpec(EmbeddingMetric, config)]
        )
    ])
    profile = why.log(obj={"col1": np.array([[0.1, 0.6], [0.1, 0.6], [0.1, 0.6]])}, schema=schema)
    view = provile.view()
    column = view.get_column("col1")
    print(column.to_summary_dict())
    assert False
