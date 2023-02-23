from typing import Any, Tuple

from fugue import DataFrame, DataFrames, Outputter
from fugue.extensions import namespace_candidate
from fugue.plugins import parse_outputter
from IPython.display import display

from whylogs.api.fugue import fugue_profile
from whylogs.viz import NotebookProfileVisualizer

from ..logger import log


class WhyViz(Outputter):
    def __init__(self, func: str, **kwargs: Any) -> None:
        super().__init__()
        assert func in ["viz"]
        self._func = func
        self._args = kwargs

    def validate_on_compile(self) -> None:
        if len(self.partition_spec.partition_by) > 0:
            raise NotImplementedError("prepartition by is not supported for visualization")

    def process(self, dfs: DataFrames) -> None:
        visualization = NotebookProfileVisualizer()
        if len(dfs) == 1:
            assert len(dfs) == 1, "viz_profile can take only one dataframe"
            p = self._profile(dfs[0])
            visualization.set_profiles(target_profile_view=p)
            display(visualization.profile_summary())
        elif len(dfs) == 2 and dfs.has_key:
            ref = self._profile(dfs["reference"])
            target = self._profile(dfs["target"])
            visualization.set_profiles(target_profile_view=target, reference_profile_view=ref)
            display(visualization.summary_drift_report())
        else:
            raise ValueError(self._func + " does not support the input")

    def _profile(self, df: DataFrame) -> Any:
        if self.execution_engine.is_distributed:
            partition = None if self.partition_spec.empty else self.partition_spec
            return fugue_profile(df, engine=self.execution_engine, partition=partition, **self._args)
        else:
            return log(df.as_pandas()).view()


@parse_outputter.candidate(namespace_candidate("why", lambda x: isinstance(x, str)))
def _parse_why_outputter(obj: Tuple[str, str], **kwargs: Any) -> Outputter:
    return WhyViz(obj[1], **kwargs)
