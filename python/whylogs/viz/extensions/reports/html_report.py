import html
from abc import ABC, abstractmethod
from typing import Any, Optional

from IPython.core.display import HTML  # type: ignore

from whylogs import DatasetProfileView
from whylogs.api.writer import Writers
from whylogs.api.writer.writer import Writable, Writer
from whylogs.viz.enums.enums import PageSpec


class HTMLReport(Writable, ABC):
    def __init__(
        self,
        ref_view: Optional[DatasetProfileView] = None,
        target_view: Optional[DatasetProfileView] = None,
        height: Optional[str] = None,
    ):
        self.ref_view = ref_view
        self.target_view = target_view
        self.height = height or None

    def writer(self, name: str = "local") -> "HTMLReportWriter":
        writer = Writers.get(name)
        return HTMLReportWriter(report=self, writer=writer)

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

    def write(self, path: Optional[str] = None, **kwargs: Any) -> None:
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
        path = path or self.get_default_path()
        _html = self.report()
        _rendered_html = _html.data
        with self._safe_open_write(path) as file:
            file.write(_rendered_html)

    def option(self):
        return self

    def get_default_path(self) -> str:
        path = "html_reports/ProfileReport.html"
        return path


class HTMLReportWriter(object):
    def __init__(self, report: HTMLReport, writer: Writer) -> None:
        self._report = report
        self._writer = writer

    def option(self, **kwargs) -> "HTMLReportWriter":
        self._writer.option(**kwargs)
        return self

    def write(self, **kwargs: Any) -> None:
        self._writer.write(file=self._report, **kwargs)
