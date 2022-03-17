import datetime
import os

import numpy as np
import pandas as pd

from whylogs import get_or_create_session
from whylogs.core.statistics.constraints import (
    DatasetConstraints,
    Op,
    SummaryConstraint,
    ValueConstraint,
    columnPairValuesInSetConstraint,
    columnsMatchSetConstraint,
    columnValuesInSetConstraint,
    columnValuesUniqueWithinRow,
    sumOfRowValuesOfMultipleColumnsEqualsConstraint,
)
from whylogs.viz import NotebookProfileViewer


def __generate_target_profile():

    session = get_or_create_session()

    with session.logger("mytestytest", dataset_timestamp=datetime.datetime(2021, 6, 2)) as logger:
        for _ in range(5):
            logger.log({"uniform_integers": np.random.randint(0, 50)})
            logger.log({"nulls": None})

        return logger.profile


def __generate_reference_profile():

    session = get_or_create_session()

    with session.logger("mytestytest", dataset_timestamp=datetime.datetime(2021, 6, 2)) as logger:
        for _ in range(5):
            logger.log({"uniform_integers": np.random.randint(0, 50)})
            logger.log({"nulls": None})

        return logger.profile


def __generate_categorical_target_profile():
    session = get_or_create_session()
    credit_cards = pd.DataFrame(
        [
            {"credit_card": "3714-496353-98431"},
            {"credit_card": "3787 344936 71000"},
            {"credit_card": "3056 930902 5904"},
            {"credit_card": "3065 133242 2899"},
        ]
    )
    return session.log_dataframe(credit_cards, "test.data")


def __generate_categorical_reference_profile():
    session = get_or_create_session()
    credit_cards = pd.DataFrame(
        [
            {"credit_card": "6011 1111 1111 1117"},
            {"credit_card": "6011-0009-9013-9424"},
            {"credit_card": "3530 1113 3330 0000"},
            {"credit_card": "3566-0020-2036-0505"},
        ]
    )
    return session.log_dataframe(credit_cards, "test.data")


def _get_sample_dataset_constraints():
    cvisc = columnValuesInSetConstraint(value_set={2, 5, 8})
    ltc = ValueConstraint(Op.LT, 1)

    min_gt_constraint = SummaryConstraint("min", Op.GT, value=100)
    max_le_constraint = SummaryConstraint("max", Op.LE, value=5)

    set1 = set(["col1", "col2"])
    columns_match_constraint = columnsMatchSetConstraint(set1)

    val_set = {(1, 2), (3, 5)}
    col_set = ["A", "B"]
    mcv_constraints = [
        columnValuesUniqueWithinRow(column_A="A", verbose=True),
        columnPairValuesInSetConstraint(column_A="A", column_B="B", value_set=val_set),
        sumOfRowValuesOfMultipleColumnsEqualsConstraint(columns=col_set, value=100),
    ]

    return DatasetConstraints(
        None,
        value_constraints={"annual_inc": [cvisc, ltc]},
        summary_constraints={"annual_inc": [max_le_constraint, min_gt_constraint]},
        table_shape_constraints=[columns_match_constraint],
        multi_column_value_constraints=mcv_constraints,
    )


def test_notebook_profile_viewer_set_profiles():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)


def test_summary_drift_report_without_preferred_height():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.summary_drift_report()


def test_summary_drift_report_with_preferred_height():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.summary_drift_report()


def test_feature_statistics_not_passing_profile_type():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.feature_statistics("uniform_integers")


def test_feature_statistics_passing_profile_type():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.feature_statistics("uniform_integers", "target")


def test_feature_statistics_passing_profile_type_and_prefered_height():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.feature_statistics("uniform_integers", "target", "1000px")


def test_download_passing_all_arguments(tmpdir):
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)

    download = viz.download(viz.summary_drift_report(), tmpdir, html_file_name="foo")
    assert os.path.exists(tmpdir + "/foo.html")


def test_constraints_report_without_preferred_height():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    dc = _get_sample_dataset_constraints()
    viz.constraints_report(dc)


def test_constraints_report_with_preferred_height():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    dc = _get_sample_dataset_constraints()
    viz.constraints_report(dc, preferred_cell_height="1000px")


def test_double_histogram_without_height():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.double_histogram("uniform_integers")


def test_double_histogram_with_height():
    target_profile = __generate_target_profile()
    reference_profile = __generate_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.double_histogram("uniform_integers", "1000px")


def test_distribution_chart_without_height():
    target_profile = __generate_categorical_target_profile()
    reference_profile = __generate_categorical_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.distribution_chart("credit_card")


def test_distribution_chart_with_height():
    target_profile = __generate_categorical_target_profile()
    reference_profile = __generate_categorical_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.distribution_chart("credit_card", "1000px")


def test_difference_distribution_chart_without_height():
    target_profile = __generate_categorical_target_profile()
    reference_profile = __generate_categorical_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.difference_distribution_chart("credit_card")


def test_difference_distribution_chart_with_height():
    target_profile = __generate_categorical_target_profile()
    reference_profile = __generate_categorical_reference_profile()
    viz = NotebookProfileViewer()
    viz.set_profiles(target_profile=target_profile, reference_profile=reference_profile)
    viz.difference_distribution_chart("credit_card", "1000px")
