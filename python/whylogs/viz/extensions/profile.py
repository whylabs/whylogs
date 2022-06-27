from whylogs.viz.extensions.reports import HTMLReport


class VizProfile(object):
    def __init__(self, report: HTMLReport):
        self.report = report

    def write(self):
        pass
