from sklearn.utils.multiclass import type_of_target
import pytest
import numpy as np

from whylogs.core.metrics.confusion_matrix import (
    ConfusionMatrix,
    SUPPORTED_TYPES,
    encode_to_integers,
)
from whylogs.proto import ScoreMatrixMessage


def test_positive_count():
    """
    check the tp and fp totals from the confusion matrix
    """
    targets = [
        ["cat", "dog", "pig"],
        ["cat", "dog"],
        [0.1, 0.4],
        [0, 1, 3],
        [True, False, True],
    ]
    predictions = [
        ["cat", "dog", "dog"],
        ["cat", "cat"],
        [0.3, 0.5],
        [0, 2, 3],
        [False, False, True],
    ]

    scores = [[0.1, 0.2, 0.4], None, [0.4, 0.3], [0.3, 0.1, 0.9], [0.2, 0.1, 0.2]]

    expectd_tp_counts = [
        {"cat": 1, "dog": 1, "pig": 0},
        {"cat": 1, "dog": 0},
        None,
        {
            0: 1,
            1: 0,
            2: 0,
            3: 1,
        },
        {True: 1, False: 1},
    ]

    expectd_fp_counts = [
        {"cat": 0, "dog": 1, "pig": 0},
        {"cat": 1, "dog": 0},
        None,
        {
            0: 0,
            1: 0,
            2: 1,
            3: 0,
        },
        {True: 0, False: 1},
    ]
    expectd_tn_counts = [
        {"cat": 1, "dog": 0, "pig": 0},
        {"cat": 1, "dog": 0},
        None,
        {
            0: 1,
            1: 0,
            2: 0,
            3: 1,
        },
        {True: 1, False: 1},
    ]

    for indx, each_targets in enumerate(targets):

        target_type = type_of_target(each_targets)
        labels = set(each_targets + predictions[indx])
        if target_type not in SUPPORTED_TYPES:
            continue
        conf_M = ConfusionMatrix(labels)
        conf_M.add(predictions[indx], each_targets, scores[indx])
        for each_ind, label in enumerate(conf_M.labels):
            # check the number of TP is correct
            assert (
                conf_M.confusion_matrix[each_ind, each_ind].floats.count
                == expectd_tp_counts[indx][label]
            )
            # check the number of FP
            sum_fp = np.sum(
                [nt.floats.count for nt in conf_M.confusion_matrix[each_ind, :]]
            )
            assert (sum_fp - expectd_tp_counts[indx][label]) == expectd_fp_counts[indx][
                label
            ]


def test_enconde_to_integer():
    with pytest.raises(ValueError):
        encode_to_integers(["1"], ["2", "3"])


def test_enconde_to_integer_should_work():
    res = encode_to_integers(["1"], ["2", "1", "3"])
    assert res[0] == 1


def test_merge_conf_matrix():
    """
    tests merging two confusion matrices of different sizes
    """
    targets_1 = ["cat", "dog", "pig"]
    targets_2 = ["cat", "dog"]

    predictions_1 = ["cat", "dog", "dog"]
    predictions_2 = ["cat", "cat"]

    scores_1 = [0.1, 0.2, 0.4]
    scores_2 = [0.4, 0.3]

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]
    expected_2 = [[1, 1], [0, 0]]
    expected_merge = [[2, 1, 0], [0, 1, 1], [0, 0, 0]]

    labels_1 = ["cat", "dog", "pig"]
    conf_M_1 = ConfusionMatrix(labels_1)
    conf_M_1.add(predictions_1, targets_1, scores_1)

    for idx, value in enumerate(conf_M_1.labels):
        for jdx, value_2 in enumerate(conf_M_1.labels):
            assert (
                conf_M_1.confusion_matrix[idx, jdx].floats.count == expected_1[idx][jdx]
            )
    labels_2 = ["cat", "dog"]
    conf_M_2 = ConfusionMatrix(labels_2)
    conf_M_2.add(predictions_2, targets_2, scores_2)

    for idx, value in enumerate(conf_M_2.labels):
        for jdx, value_2 in enumerate(conf_M_2.labels):
            assert (
                conf_M_2.confusion_matrix[idx, jdx].floats.count == expected_2[idx][jdx]
            )

    new_conf = conf_M_1.merge(conf_M_2)

    print(new_conf.labels)
    for idx, value in enumerate(new_conf.labels):
        for jdx, value_2 in enumerate(new_conf.labels):
            print(idx, jdx)
            assert (
                new_conf.confusion_matrix[idx, jdx].floats.count
                == expected_merge[idx][jdx]
            )


def test_confusion_matrix_to_protobuf():
    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    labels_1 = ["cat", "dog", "pig"]
    conf_M_1 = ConfusionMatrix(labels_1)
    conf_M_1.add(predictions_1, targets_1, scores_1)
    message = conf_M_1.to_protobuf()

    expected_1 = [[1, 0, 0], [0, 1, 1], [0, 0, 0]]

    new_conf = ConfusionMatrix.from_protobuf(message)
    for idx, value in enumerate(new_conf.labels):
        assert value == conf_M_1.labels[idx]

    for idx, value in enumerate(new_conf.labels):
        for jdx, value_2 in enumerate(new_conf.labels):
            assert (
                new_conf.confusion_matrix[idx, jdx].floats.count == expected_1[idx][jdx]
            )


def test_parse_empty_protobuf_should_return_none():
    empty_message = ScoreMatrixMessage()
    assert ConfusionMatrix.from_protobuf(empty_message) is None


def test_merge_with_none():
    targets_1 = ["cat", "dog", "pig"]
    predictions_1 = ["cat", "dog", "dog"]
    scores_1 = [0.1, 0.2, 0.4]

    labels_1 = ["cat", "dog", "pig"]
    matrix = ConfusionMatrix(labels_1)
    matrix.add(predictions_1, targets_1, scores_1)

    res = matrix.merge(other_cm=None)
    assert res.target_field is None
    assert res.prediction_field is None
    assert res.score_field is None
