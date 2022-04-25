import os

import pandas as pd

import whylogs as ylog
from whylogs.viewer.jupyter_notebook_viz import NotebookProfileVisualizer


def test_viz() -> None:
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [4.3, 1.8, None, 4.1],
    }

    df = pd.DataFrame(data)

    results = ylog.log(pandas=df)
    profile = results.get_profile()

    visualization = NotebookProfileVisualizer()
    visualization.set_profiles(target_profile=profile, reference_profile=profile)

    visualization.write(
        rendered_html=visualization.feature_statistics(feature_name="weight", profile="target"),
        html_file_name=os.getcwd() + "/b18",
    )
