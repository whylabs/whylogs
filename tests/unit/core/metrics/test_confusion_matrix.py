from whylogs.core.metrics_profiles import Model, SUPPORTED_TYPES
from sklearn.utils.multiclass import type_of_target
import pytest
import numpy as np
from whylogs.core.metrics.confusion_matrix import ConfusionMatrix


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
            continue
        conf_M = ConfusionMatrix(labels)
        conf_M.add(predictions[indx], each_targets, scores[indx])
        for each_ind, label in enumerate(conf_M.labels):
            assert conf_M.confusion_matrix[each_ind,
                                           each_ind].floats.count == expectd_tp_counts[indx][label]

            sum_fp = np.sum(
                [nt.floats.count for nt in conf_M.confusion_matrix[each_ind, :]])
            assert (sum_fp-expectd_tp_counts[indx]
                    [label]) == expectd_fp_counts[indx][label]
