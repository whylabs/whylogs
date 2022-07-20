import os
import webbrowser

import pytest
from python.whylogs.core.constraints import Constraints

from whylogs.core import DatasetProfileView
from whylogs.viz import NotebookProfileVisualizer


@pytest.fixture()
def visualization() -> NotebookProfileVisualizer:
    visualization = NotebookProfileVisualizer()
    return visualization


def test_viz_feature_statistics(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer, tmp_path: str
) -> None:
    visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.feature_statistics(feature_name="weight", profile="target"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_double_histogram_single_profile(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer, tmp_path: str
) -> None:
    visualization.set_profiles(target_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.double_histogram(feature_name="weight"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_double_histogram_two_profiles(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer, tmp_path: str
) -> None:
    visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.double_histogram(feature_name="weight"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_distribution_chart_single_profile(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer, tmp_path: str
) -> None:
    visualization.set_profiles(target_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.distribution_chart(feature_name="animal"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_distribution_chart_two_profiles(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer, tmp_path: str
) -> None:
    visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.distribution_chart(feature_name="animal"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_difference_distribution_chart_two_profiles(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer, tmp_path: str
) -> None:
    visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.difference_distribution_chart(feature_name="animal"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_profile_summary(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer, tmp_path: str
) -> None:
    visualization.set_profiles(target_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.profile_summary(),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_summary_drift(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer, tmp_path: str
) -> None:
    visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.summary_drift_report(),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_summary_drift_if_view_is_none(
    profile_view: DatasetProfileView, visualization: NotebookProfileVisualizer
) -> None:
    visualization.set_profiles(target_profile_view=profile_view)
    with pytest.raises(ValueError):
        visualization.summary_drift_report()


def test_viz_constraints_report(
    profile_view: DatasetProfileView,
    visualization: NotebookProfileVisualizer,
    max_less_than_equal_constraints: Constraints,
    tmp_path: str,
) -> None:

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.constraints_report(max_less_than_equal_constraints),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)
