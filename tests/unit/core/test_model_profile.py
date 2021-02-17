from whylogs.core.model_profile import ModelProfile


def test_model_profile():
    mod_prof = ModelProfile()
    assert mod_prof.output_fields == []
    assert mod_prof.metrics is not None
    assert mod_prof.metrics.confusion_matrix is not None
    message = mod_prof.to_protobuf()
    model_profile = ModelProfile.from_protobuf(message)


def test_model_profile():
    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]
    mod_prof = ModelProfile()

    assert mod_prof.output_fields == []

    mod_prof.compute_metrics(predictions_1, targets_1, scores_1)
    assert mod_prof.metrics is not None
    assert mod_prof.metrics.confusion_matrix is not None
    message = mod_prof.to_protobuf()
    model_profile = ModelProfile.from_protobuf(message)
    assert model_profile.metrics is not None
    assert mod_prof.metrics.confusion_matrix is not None
    assert mod_prof.metrics.confusion_matrix.labels is not None
    assert model_profile.metrics.confusion_matrix.labels == mod_prof.metrics.confusion_matrix.labels


def test_merge_profile():

    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]
    mod_prof = ModelProfile()

    assert mod_prof.output_fields == []

    mod_prof.compute_metrics(predictions_1, targets_1, scores_1)
    assert mod_prof.metrics is not None

    mod_prof_2 = ModelProfile()
    assert mod_prof_2.output_fields == []

    mod_prof_3 = mod_prof.merge(mod_prof_2)
