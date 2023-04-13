import os
import tempfile

import pandas as pd

import whylogs as why
from whylogs.core.metrics.metrics import MetricConfig
from whylogs.core.preprocessing import PreprocessedColumn
from whylogs.core.resolvers import MetricSpec, ResolverSpec
from whylogs.core.schema import DeclarativeSchema
from whylogs.experimental.extras.nlp_metric import (
    BagOfWordsConfig,
    BagOfWordsMetric,
    get_vocabulary,
    save_vocabulary,
)


def test_bag_of_words_list() -> None:
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric, BagOfWordsConfig())])]
    schema = DeclarativeSchema(resolvers)
    profile = why.log(row={"words": ["foo", "bar", "wubbie"]}, schema=schema)
    view = profile.view()
    column = view.get_column("words")
    summary = column.to_summary_dict()
    assert summary["nlp_bow/doc_length:distribution/mean"] == 3
    assert "nlp_bow/out_of_vocab/distribution/n" not in summary
    assert "nlp_bow/frequent_terms:frequent_items/frequent_strings" in summary


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
    assert "nlp_bow/frequent_terms:frequent_items/frequent_strings" in summary


def test_oov_known_vocab() -> None:
    vocab = ["a", "b", "c"]
    config = BagOfWordsConfig.set_vocabulary(vocab)
    assert len(config._vocabulary) == len(vocab)
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric, config)])]
    schema = DeclarativeSchema(resolvers)
    doc = ["a", "b", "c", "1", "2", "a", "c", "2"]
    profile = why.log(row={"words": doc}, schema=schema)
    assert len(config._vocabulary) == len(vocab)
    view = profile.view()
    column = view.get_column("words")
    summary = column.to_summary_dict()

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


def test_oov_learn_vocab() -> None:
    config = BagOfWordsConfig.init_vocabulary()
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric, config)])]
    schema = DeclarativeSchema(resolvers)
    doc = ["a", "b", "c", "1", "2", "a", "c", "2"]
    profile = why.log(row={"words": doc}, schema=schema)

    view = profile.view()
    column = view.get_column("words")
    assert len(column.get_metric("nlp_bow").vocabulary) == len(set(doc))
    summary = column.to_summary_dict()

    assert summary["nlp_bow/out_of_vocab:distribution/n"] == 1
    assert summary["nlp_bow/out_of_vocab:distribution/min"] == 0
    assert summary["nlp_bow/out_of_vocab:distribution/max"] == 0
    assert summary["nlp_bow/out_of_vocab:ints/min"] == 0
    assert summary["nlp_bow/out_of_vocab:ints/max"] == 0
    assert summary["nlp_bow/out_of_vocab:counts/n"] == 1

    assert summary["nlp_bow/doc_length:distribution/min"] == len(doc)
    assert summary["nlp_bow/doc_length:distribution/mean"] == len(doc)
    assert summary["nlp_bow/doc_length:distribution/max"] == len(doc)
    assert summary["nlp_bow/doc_length:distribution/n"] == 1
    assert summary["nlp_bow/doc_length:counts/n"] == 1
    assert summary["nlp_bow/doc_length:ints/min"] == len(doc)
    assert summary["nlp_bow/doc_length:ints/max"] == len(doc)


def test_oov_serialization() -> None:
    with tempfile.NamedTemporaryFile(delete=False) as f:
        filename = f.name

    vocab = ["a", "b", "c"]
    config1 = BagOfWordsConfig.set_vocabulary(vocab)
    save_vocabulary(config1._vocabulary, filename)
    config2 = BagOfWordsConfig.load_vocabulary(filename)

    assert len(config1._vocabulary) == len(config2._vocabulary) == len(vocab)
    for term in vocab:
        assert term in config1._vocabulary
        assert term in config2._vocabulary

    os.remove(filename)


def test_oov_merge() -> None:
    base_config = MetricConfig(fi_disabled=True)
    config = BagOfWordsConfig.init_vocabulary(config=base_config)
    assert config.fi_disabled
    assert config.update_vocab

    doc1 = ["a", "b", "c"]
    doc2 = ["1", "2", "3", "4", "5"]
    metric1 = BagOfWordsMetric.zero(config)
    metric2 = BagOfWordsMetric.zero(config)
    metric1.columnar_update(PreprocessedColumn.apply([doc1]))
    assert len(metric1.vocabulary) == len(doc1)
    summary1 = metric1.to_summary_dict()
    assert "frequent_terms:frequent_strings" not in summary1
    metric2.columnar_update(PreprocessedColumn.apply([doc2]))
    assert len(metric2.vocabulary) == len(doc2)
    summary2 = metric2.to_summary_dict()
    assert "frequent_terms:frequent_strings" not in summary2

    merged = metric1.merge(metric2)
    for t in doc1 + doc2:
        assert t in merged.vocabulary
    merged_summary = merged.to_summary_dict()
    assert "frequent_terms:frequent_strings" not in merged_summary


def test_profile_vocab() -> None:
    config = BagOfWordsConfig.init_vocabulary()
    resolvers = [ResolverSpec(column_name="words", metrics=[MetricSpec(BagOfWordsMetric, config)])]
    schema = DeclarativeSchema(resolvers)
    doc = ["foo", "bar", "wubbie"]
    profile = why.log(row={"words": doc}, schema=schema).profile()
    vocab = get_vocabulary(profile, "words")
    for t in doc:
        assert t in vocab

    view = profile.view()
    vocab = get_vocabulary(view, "words")
    for t in doc:
        assert t in vocab
