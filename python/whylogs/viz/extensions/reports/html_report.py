import html
from abc import ABC, abstractmethod
from typing import Any, Optional

from IPython.core.display import HTML

from whylogs import DatasetProfileView
from whylogs.api.writer import Writers
from whylogs.api.writer.writer import Writable
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
    def report(self) -> str:
        pass

    def write(self, path: str) -> None:
        """Create HTML file for a given report.

        Parameters
        ----------
        path: str
            The path where the HTML reports will be stored to.

        Examples
        --------
        .. code-block:: python

            from whylogs.viz import VizProfile, SummaryDriftReport

            report = SummaryDriftReport.report()

            viz_profile = VizProfile(report=report)
            viz_profile.write(path="path/to/report/Report.html")
        """
        _rendered_html = self.report()
        with self._safe_open_write(path) as file:
            file.write(_rendered_html)

    def option(self, **kwargs: Any):
        return self


class HTMLReportWriter(Writable):
    def __init__(self, report: HTMLReport, writer: Writable) -> None:
        self._report = report
        self._writer = writer

    def option(self, **kwargs) -> "HTMLReportWriter":
        self._writer.option(**kwargs)
        return self

    def write(self, **kwargs) -> None:
        self._writer.write(file=self._report, **kwargs)
