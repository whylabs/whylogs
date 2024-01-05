# flake8: noqa
from whylogs.api.usage_stats import emit_usage

# This import has a side effect
from .profiler import fugue_profile  # type: ignore

emit_usage("fugue")
