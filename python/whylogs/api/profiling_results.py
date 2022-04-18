from whylogs.core import DatasetProfile, DatasetProfileView


class ProfilingResults:
    """
    A holder object for profiling results.

    A whylogs.log call can result to more than one profiles. This wrapper class
    simplifies the navigation among these profiles.

    Note that currently we only hold one profile but we're planning to add other
    kind of profiles here.
    """

    def __init__(self, profile: DatasetProfile):
        self._profile = profile

    def get(self) -> DatasetProfile:
        return self._profile

    def view(self) -> DatasetProfileView:
        return self._profile.view()
