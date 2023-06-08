# flake8: noqa
from whylogs.api.usage_stats import emit_usage

from .profiler import fugue_profile

emit_usage("fugue")
