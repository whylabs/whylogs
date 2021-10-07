import pytest

from whylogs.core.metrics.nlp_metrics import NLPMetrics


def test_nlp_metrics():
    nlp_metric = NLPMetrics()
    assert nlp_metric.mer is not None
    assert nlp_metric.mer.count == 0
    nlp_metric.update("hello", "hello")

    assert nlp_metric.mer.histogram.get_min_value() == 0.0
    assert nlp_metric.mer.histogram.get_max_value() == 0.0
    assert nlp_metric.mer.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.0, 0.0, 0.0]
    assert nlp_metric.mer.count == 1

    assert nlp_metric.wer.histogram.get_min_value() == 0.0
    assert nlp_metric.wer.histogram.get_max_value() == 0.0
    assert nlp_metric.wer.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.0, 0.0, 0.0]
    assert nlp_metric.wer.count == 1

    assert nlp_metric.wil.histogram.get_min_value() == 0.0
    assert nlp_metric.wil.histogram.get_max_value() == 0.0
    assert nlp_metric.wil.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.0, 0.0, 0.0]
    assert nlp_metric.wil.count == 1


def test_nlp_metrics_message():
    nlp_metric = NLPMetrics()
    assert nlp_metric.mer is not None
    assert nlp_metric.mer.count == 0
    nlp_metric.update(["hello brother"], ["hello sister"])
    nlp_metric.update(["bye brother"], ["bye sister"])
    nlp_metric.update(["what up brother"], ["what up ii sister"])
    assert nlp_metric.mer.histogram.get_min_value() == 0.5
    assert nlp_metric.mer.histogram.get_max_value() == 0.5
    assert nlp_metric.mer.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.5, 0.5, 0.5000]
    assert nlp_metric.mer.count == 3

    assert nlp_metric.wer.histogram.get_min_value() == 0.5
    assert nlp_metric.wer.histogram.get_max_value() == 0.5
    assert nlp_metric.wer.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.5, 0.5, 0.5000]
    assert nlp_metric.wer.count == 3

    assert pytest.approx(nlp_metric.wil.histogram.get_min_value(), 0.001) == 0.6666
    assert nlp_metric.wil.histogram.get_max_value() == 0.75
    assert pytest.approx(nlp_metric.wil.histogram.get_quantiles([0.25, 0.5, 0.75]), 0.001) == [0.666666, 0.75, 0.75]
    assert nlp_metric.wil.count == 3

    nlp_message = nlp_metric.to_protobuf()

    nlp_metric_new = NLPMetrics.from_protobuf(nlp_message)

    assert nlp_metric_new.wil.histogram.get_min_value() == nlp_metric.wil.histogram.get_min_value()
    assert nlp_metric_new.wil.histogram.get_max_value() == nlp_metric.wil.histogram.get_max_value()
    assert nlp_metric_new.wil.histogram.get_quantiles([0.25, 0.5, 0.75]) == nlp_metric.wil.histogram.get_quantiles([0.25, 0.5, 0.75])
    assert nlp_metric_new.wil.count == nlp_metric.wil.count

    assert nlp_metric_new.mer.histogram.get_min_value() == nlp_metric.mer.histogram.get_min_value()
    assert nlp_metric_new.mer.histogram.get_max_value() == nlp_metric.mer.histogram.get_max_value()
    assert nlp_metric_new.mer.histogram.get_quantiles([0.25, 0.5, 0.75]) == nlp_metric.mer.histogram.get_quantiles([0.25, 0.5, 0.75])
    assert nlp_metric_new.mer.count == nlp_metric.mer.count

    assert nlp_metric_new.wer.histogram.get_min_value() == nlp_metric.wer.histogram.get_min_value()
    assert nlp_metric_new.wer.histogram.get_max_value() == nlp_metric.wer.histogram.get_max_value()
    assert nlp_metric_new.wer.histogram.get_quantiles([0.25, 0.5, 0.75]) == nlp_metric.wer.histogram.get_quantiles([0.25, 0.5, 0.75])
    assert nlp_metric_new.wer.count == nlp_metric.wer.count


def test_nlp_merge():
    nlp_metric = NLPMetrics()
    nlp_metric.update("hello", "hello")

    nlp_initial_test = None
    nlp_merge_metrics = nlp_metric.merge(nlp_initial_test)
    assert nlp_merge_metrics.mer.histogram.get_min_value() == 0.0
    assert nlp_merge_metrics.mer.histogram.get_max_value() == 0.0
    assert nlp_merge_metrics.mer.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.0, 0.0, 0.0]
    assert nlp_merge_metrics.mer.count == 1

    assert nlp_merge_metrics.wer.histogram.get_min_value() == 0.0
    assert nlp_merge_metrics.wer.histogram.get_max_value() == 0.0
    assert nlp_merge_metrics.wer.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.0, 0.0, 0.0]
    assert nlp_merge_metrics.wer.count == 1

    assert nlp_merge_metrics.wil.histogram.get_min_value() == 0.0
    assert nlp_merge_metrics.wil.histogram.get_max_value() == 0.0
    assert nlp_merge_metrics.wil.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.0, 0.0, 0.0]
    assert nlp_merge_metrics.wil.count == 1

    nlp_metric_2 = NLPMetrics()

    nlp_metric_2.update(["hello brother"], ["hello sister"])
    nlp_metric_2.update(["bye brother"], ["bye sister"])
    nlp_metric_2.update(["what up brother"], ["what up ii sister"])

    nlp_merge_metrics = nlp_merge_metrics.merge(nlp_metric_2)

    assert nlp_merge_metrics.mer.histogram.get_min_value() == 0.0
    assert nlp_merge_metrics.mer.histogram.get_max_value() == 0.5
    assert nlp_merge_metrics.mer.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.5, 0.5, 0.5000]
    assert nlp_merge_metrics.mer.count == 4

    assert nlp_merge_metrics.wer.histogram.get_min_value() == 0.0
    assert nlp_merge_metrics.wer.histogram.get_max_value() == 0.5
    assert nlp_merge_metrics.wer.histogram.get_quantiles([0.25, 0.5, 0.75]) == [0.5, 0.5, 0.5000]
    assert nlp_merge_metrics.wer.count == 4

    assert pytest.approx(nlp_merge_metrics.wil.histogram.get_min_value(), 0.001) == 0.0
    assert nlp_merge_metrics.wil.histogram.get_max_value() == 0.75
    assert pytest.approx(nlp_merge_metrics.wil.histogram.get_quantiles([0.25, 0.5, 0.75]), 0.001) == [0.666666, 0.75, 0.75]
    assert nlp_merge_metrics.wil.count == 4
