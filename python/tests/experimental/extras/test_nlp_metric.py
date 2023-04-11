import pandas as pd

import whylogs as why
from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.experimental.extras.nlp_metric import BagOfWordsConfig, BagOfWordsMetric


def test_bag_of_words_list() -> None:
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric, BagOfWordsConfig())])]
    schema = DeclarativeSchema(resolvers)
    profile = why.log(row={"words": ["foo", "bar", "wubbie"]}, schema=schema)
    view = profile.view()
    column = view.get_column("words")
    summary = column.to_summary_dict()
    assert summary["nlp_bow/doc_length:distribution/mean"] == 3
    assert "nlp_bow/out_of_vocab/distribution/n" not in summary


def test_bag_of_words_pandas() -> None:
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric, BagOfWordsConfig())])]
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
    assert "nlp_bow/out_of_vocab/distribution/n" not in summary


def test_bag_of_words_oov() -> None:
    vocab = ["a", "b", "c"]
    config = BagOfWordsConfig.set_vocabulary(vocab)
    assert len(config.vocabulary) == len(vocab)
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric, config)])]
    schema = DeclarativeSchema(resolvers)
    doc = ["a", "b", "c", "1", "2", "a", "c", "2"]
    profile = why.log(row={"words": doc}, schema=schema)
    view = profile.view()
    column = view.get_column("words")
    summary = column.to_summary_dict()
    print(summary)
    assert summary["nlp_bow/out_of_vocab:distribution/n"] == 1
    assert summary["nlp_bow/out_of_vocab:distribution/min"] == 3
    assert summary["nlp_bow/out_of_vocab:distribution/max"] == 3
    assert summary["nlp_bow/out_of_vocab:ints/min"] == 3
    assert summary["nlp_bow/out_of_vocab:ints/max"] == 3
    assert summary["nlp_bow/out_of_vocab:counts/n"] == 1

    assert summary["nlp_bow/doc_length:distribution/min"] == len(doc)
    assert summary["nlp_bow/doc_length:distribution/mean"] == len(doc)
    assert summary["nlp_bow/doc_length:distribution/max"] == len(doc)
    assert summary["nlp_bow/doc_length:distribution/n"] == 1
    assert summary["nlp_bow/doc_length:counts/n"] == 1
    assert summary["nlp_bow/doc_length:ints/min"] == len(doc)
    assert summary["nlp_bow/doc_length:ints/max"] == len(doc)
