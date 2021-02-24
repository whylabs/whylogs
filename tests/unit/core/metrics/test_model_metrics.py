from whylogs.core.metrics.model_metrics import ModelMetrics


def tests_model_metrics():
    mod_met = ModelMetrics()

    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]

    mod_met.compute_confusion_matrix(predictions_1, targets_1, scores_1)

    print(mod_met.confusion_matrix.labels)
    for idx, value in enumerate(mod_met.confusion_matrix.labels):
        for jdx, value_2 in enumerate(mod_met.confusion_matrix.labels):
            print(idx, jdx)
            assert mod_met.confusion_matrix.confusion_matrix[idx,
                                                             jdx].floats.count == expected_1[idx][jdx]


def tests_model_metrics_to_protobuf():
    mod_met = ModelMetrics()

    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]

    mod_met.compute_confusion_matrix(predictions_1, targets_1, scores_1)

    message = mod_met.to_protobuf()

    ModelMetrics.from_protobuf(message)


def test_merge_none():
    metrics = ModelMetrics()
    metrics.merge(None)


def test_merge_metrics_with_none_confusion_matrix():
    metrics = ModelMetrics()
    other = ModelMetrics()
    other.confusion_matrix = None
    metrics.merge(other)
