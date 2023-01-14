import numpy as np

import whylogs as why

from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.experimental.extras.nlp_metric import BagOfWordsMetric


def test_bag_of_words_meric_holds_the_smoke_in() -> None:
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric)])]
    schema = DeclarativeSchema(resolvers)
    profile = why.log(obj={"words": np.array(["foo", "bar", "wubbie"])}, schema=schema)
    view = profile.view()
    column = view.get_column("words")
    summary = column.to_summary_dict()
    assert summary["nlp_bow/doc_length:distribution/mean"] == 3
