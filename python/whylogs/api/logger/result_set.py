from abc import ABC, abstractmethod
from typing import Any

from whylogs.api.writer import Writer, Writers
from whylogs.core import DatasetProfile, DatasetProfileView


class ResultSet(ABC):
    """
    A holder object for profiling results.

    A whylogs.log call can result in more than one profile. This wrapper class
    simplifies the navigation among these profiles.

    Note that currently we only hold one profile but we're planning to add other
    kinds of profiles such as segmented profiles here.
    """

    @classmethod
    def read(cls, multi_profile_file: str) -> "ResultSet":
        # TODO: parse multiple profile
        view = DatasetProfileView.read(multi_profile_file)
        return ViewResultSet(view=view)

    def writer(self, name: str = "local") -> "ResultSetWriter":
        writer = Writers.get(name)
        return ResultSetWriter(results=self, writer=writer)

    @abstractmethod
    def view(self) -> DatasetProfileView:
        pass

    @abstractmethod
    def profile(self) -> DatasetProfile:
        pass


class ProfileResultSet(ResultSet):
    def __init__(self, profile: DatasetProfile) -> None:
        self._profile = profile

    def profile(self) -> DatasetProfile:
        return self._profile

    def view(self) -> DatasetProfileView:
        return self._profile.view()


class ViewResultSet(ResultSet):
    def __init__(self, view: DatasetProfileView) -> None:
        self._view = view

    def profile(self) -> DatasetProfile:
        raise ValueError("No profile available. Can only view")

    def view(self) -> DatasetProfileView:
        return self._view


class ResultSetWriter:
    """
    Result of a logging call.

    A result set might contain one or multiple profiles or profile views.
    """

    def __init__(self, results: ResultSet, writer: Writer):
        self._result_set = results
        self._writer = writer

    def option(self, **kwargs: Any) -> "ResultSetWriter":
        self._writer.option(**kwargs)
        return self

    def write(self, **kwargs) -> None:
        # TODO: multi-profile writer
        view = self._result_set.view()
        self._writer.write(profile=view, **kwargs)
