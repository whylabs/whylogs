import pandas as pd

import whylogs as why
from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.experimental.extras.nlp_metric import BagOfWordsMetric


def test_bag_of_words_list() -> None:
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric)])]
    schema = DeclarativeSchema(resolvers)
    profile = why.log(row={"words": ["foo", "bar", "wubbie"]}, schema=schema)
    view = profile.view()
    column = view.get_column("words")
    summary = column.to_summary_dict()
    assert summary["nlp_bow/doc_length:distribution/mean"] == 3


def test_bag_of_words_pandas() -> None:
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric)])]
    schema = DeclarativeSchema(resolvers)
    profile = why.log(
        pandas=pd.DataFrame({"words": [["foo", "bar", "wubbie"], ["foo", "bar"], ["wubbie"]]}), schema=schema
    )
    view = profile.view()
    column = view.get_column("words")
    summary = column.to_summary_dict()
    assert summary["nlp_bow/doc_length:distribution/min"] == 1
    assert summary["nlp_bow/doc_length:distribution/mean"] == 2
    assert summary["nlp_bow/doc_length:distribution/max"] == 3
    assert summary["nlp_bow/doc_length:distribution/n"] == 3
