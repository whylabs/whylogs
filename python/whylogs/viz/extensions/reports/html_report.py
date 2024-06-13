import html
import os
from abc import ABC, abstractmethod
from typing import Any, List, Optional, Tuple, Union

from IPython.core.display import HTML  # type: ignore

from whylogs import DatasetProfileView
from whylogs.api.writer.writer import WriterWrapper, _Writable
from whylogs.viz.enums.enums import PageSpec


class HTMLReport(_Writable, ABC):
    def __init__(
        self,
        ref_view: Optional[DatasetProfileView] = None,
        target_view: Optional[DatasetProfileView] = None,
        height: Optional[str] = None,
    ):
        self.ref_view = ref_view
        self.target_view = target_view
        self.height = height or None

    def display(self, template: str, page_spec: PageSpec) -> HTML:
        if not self.height:
            self.height = page_spec.height
        iframe = f"""<div></div><iframe srcdoc="{html.escape(template)}" width=100% height={self.height}
        frameBorder=0></iframe>"""
        display = HTML(iframe)
        return display

    @abstractmethod
    def report(self) -> HTML:
        pass

    def _write(
        self, path: Optional[str] = None, filename: Optional[str] = None, **kwargs: Any
    ) -> Tuple[bool, Union[str, List[str]]]:
        """Create HTML file for a given report.

        Parameters
        ----------
        path: str, optional
            The path where the HTML reports will be stored to.

        Examples
        --------
        .. code-block:: python

            from whylogs.viz import VizProfile, SummaryDriftReport

            report = SummaryDriftReport.report()

            viz_profile = VizProfile(report=report)
            viz_profile.write(path="path/to/report/Report.html")
        """
        path = path or self._get_default_path()
        filename = filename or self._get_default_filename()
        path = os.path.join(path, filename) if path else filename
        _html = self.report()
        _rendered_html = _html.data
        with self._safe_open_write(path) as file:
            file.write(_rendered_html)
            return True, file.name

    def option(self):
        return self

    def _get_default_filename(self) -> str:
        return "html_reports/ProfileReport.html"


HTMLReportWriter = WriterWrapper
