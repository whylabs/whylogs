import numpy as np

import whylogs as why
from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.experimental.core.metrics.embedding_metric import (
    DistanceFunction,
    EmbeddingConfig,
    EmbeddingMetric,
)


def test_embedding_metric_holds_the_smoke_in() -> None:
    config = EmbeddingConfig(
        references=np.array([[0.01, 0.01, 0.01], [1, 1, 1]]),
        labels=["A", "B"],
        distance_fn=DistanceFunction.euclidean,
    )
    schema = DeclarativeSchema([ResolverSpec(column_name="col1", metrics=[MetricSpec(EmbeddingMetric, config)])])
    sample_data = np.array([[0.1, 0.1, 0.1], [0.6, 0.6, 0.6], [2, 2, 2]])
    profile = why.log(row={"col1": sample_data}, schema=schema)
    view = profile.view()
    column = view.get_column("col1")
    summary = column.to_summary_dict()
    print(summary)
    assert summary["embedding/A_distance:counts/n"] == 3
    assert summary["embedding/B_distance:counts/n"] == 3
    assert summary["embedding/A_distance:distribution/mean"] > 0
    assert summary["embedding/B_distance:distribution/mean"] > 0
    assert summary["embedding/closest:counts/n"] == 3
    assert False
