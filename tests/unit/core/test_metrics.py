from whylogs.core.metrics_profiles import Model, SUPPORTED_TYPES
from sklearn.utils.multiclass import type_of_target
import pytest
import numpy as np


def test_positive_count():

    targets = [["cat", "dog", "pig"],
               ["cat", "dog"],
               [0.1, 0.4],
               [0, 1, 3],
               [True, False, True]
               ]
    predictions = [["cat", "dog", "dog"],
                   ["cat", "cat"],
                   [0.3, 0.5],
                   [0, 2, 3],
                   [False, False, True],
                   ]

    scores = [[0.1, 0.2, 0.4],
              None,
              [0.4, 0.3],
              [0.3, 0.1, 0.9],
              [0.2, 0.1, 0.2]]

    expectd_tp_counts = [{
        "cat": 1,
        "dog": 1,
        "pig": 0
    }, {
        "cat": 1,
        "dog": 0},
        None,
        {
        0: 1,
        1: 0,
        2: 0,
        3: 1,
    }, {
        True: 1,
        False: 1
    }]

    expectd_fp_counts = [{
        "cat": 0,
        "dog": 1,
        "pig": 0
    }, {
        "cat": 1,
        "dog": 0},
        None,
        {
        0: 0,
        1: 0,
        2: 1,
        3: 0,
    }, {
        True: 0,
        False: 1
    }]
    expectd_tn_counts = [{
        "cat": 1,
        "dog": 0,
        "pig": 0
    }, {
        "cat": 1,
        "dog": 0},
        None,
        {
        0: 1,
        1: 0,
        2: 0,
        3: 1,
    }, {
        True: 1,
        False: 1
    }]

    for indx, each_targets in enumerate(targets):

        target_type = type_of_target(each_targets)
        labels = set(each_targets + predictions[indx])
        if target_type not in SUPPORTED_TYPES:
            with pytest.raises(NotImplementedError):
                mod = Model("test_model_{}".format(indx))
                mod.track_metrics(
                    each_targets, predictions[indx], scores[indx])

        else:
            mod = Model("test_model_{}".format(indx))
            mod.track_metrics(each_targets, predictions[indx], scores[indx])
            for each_label in labels:

                tp_profile = mod.profiles.get(
                    "whylogs.metrics.true_positive_scores.{}".format(each_label), None)
                fp_profile = mod.profiles.get(
                    "whylogs.metrics.false_positive_scores.{}".format(each_label), None)
                tn_profile = mod.profiles.get(
                    "whylogs.metrics.true_negative_scores.{}".format(each_label), None)
                fn_profile = mod.profiles.get(
                    "whylogs.metrics.false_negative_scores.{}".format(each_label), None)

                assert tp_profile is not None
                assert fp_profile is not None
                assert tn_profile is not None
                assert fn_profile is not None

                tp_counts = tp_profile.number_tracker.floats.count

                fp_counts = fp_profile.number_tracker.floats.count

                assert tp_counts == expectd_tp_counts[indx].get(
                    each_label, None)
                assert fp_counts == expectd_fp_counts[indx].get(
                    each_label, None)
