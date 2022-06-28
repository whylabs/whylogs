import html
from typing import Optional, Any
from abc import ABC, abstractmethod

from IPython.core.display import HTML

from whylogs.api.writer import Writers
from whylogs import DatasetProfileView
from whylogs.viz.enums.enums import PageSpec
from whylogs.api.writer.writer import Writable


class HTMLReport(ABC):
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


class HTMLReportWriter(Writable):
    def __init__(self, report: Optional[HTMLReport] = None, writer: Optional[Writable] = None) -> None:
        self._report = report.report()
        self._writer = writer

    def option(self, **kwargs: Any) -> "HTMLReportWriter":
        self._writer.option(**kwargs)
        return self

    def write(self, **kwargs) -> None:
        self._writer.write(file=self._report, **kwargs)