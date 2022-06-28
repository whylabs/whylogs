import os
from typing import Optional

from whylogs.api.writer.writer import Writable


class VizProfileWriter(Writable):
    def __init__(self, rendered_html: str):
        self.rendered_html = rendered_html

    def write(self, path: str, file_name: Optional[str] = None) -> None:
        """Create HTML file for a given report.

        Parameters
        ----------
        path: str
            The path where the profiles should be stored. 
        file_name: str, optional
            Name for the created HTML file. If none is passed, created HTML will be named `ProfileVisualizer.html`

        Examples
        --------
        .. code-block:: python

            from whylogs.viz import VizProfile, SummaryDriftReport

            report = SummaryDriftReport.report()

            viz_profile = VizProfile(report=report)
            viz_profile.write(path="path/to/report", file_name="my_file")
        """
        file_name = file_name or "ProfileVisualizer"
        full_path = os.path.join(path, str(file_name) + ".html")
        with self._safe_open_write(os.path.abspath(full_path)) as file:
            file.write(self.rendered_html)


if __name__ == "__main__":
    import whylogs as why
    import pandas as pd
    from whylogs.viz.extensions.reports.summary_drift import SummaryDriftReport
    data = {
        "animal": ["cat", "hawk", "snake", "cat"],
        "legs": [4, 2, 0, 4],
        "weight": [4.3, 1.8, None, 4.1],
    }

    df = pd.DataFrame(data)

    result_set = why.log(df)

    report = SummaryDriftReport(ref_view=result_set.view(), target_view=result_set.view())
    
    report.writer("local").write()

