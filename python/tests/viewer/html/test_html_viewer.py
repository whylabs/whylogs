import os
import webbrowser

from whylogs.viewer import NotebookProfileVisualizer


def test_viz_feature_statistics(profile_view, tmp_path: str) -> None:
    visualization = NotebookProfileVisualizer()
    visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.feature_statistics(feature_name="weight", profile="target"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_double_histogram_single_profile(profile_view, tmp_path: str) -> None:
    visualization = NotebookProfileVisualizer()
    visualization.set_profiles(target_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.double_histogram(feature_name="weight"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_double_histogram_two_profiles(profile_view, tmp_path: str) -> None:
    visualization = NotebookProfileVisualizer()
    visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.double_histogram(feature_name="weight"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_distribution_chart_single_profile(profile_view, tmp_path: str) -> None:
    visualization = NotebookProfileVisualizer()
    visualization.set_profiles(target_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.distribution_chart(feature_name="animal"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)


def test_viz_distribution_chart_two_profiles(profile_view, tmp_path: str) -> None:
    visualization = NotebookProfileVisualizer()
    visualization.set_profiles(target_profile_view=profile_view, reference_profile_view=profile_view)

    test_output = os.path.join(tmp_path, "b18")
    visualization.write(
        rendered_html=visualization.distribution_chart(feature_name="animal"),
        html_file_name=test_output,
    )
    webbrowser.open(f"file://{os.path.realpath(test_output)}.html", new=2)
