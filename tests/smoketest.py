"""
This is here to verify that the produced wheel includes
all the necessary dependencies. This is excersized by
the CI workflow and does not use pytest because it is
intended to test the wheel in a production environment,
not a development environment.
"""

from whylogs.core import ColumnProfile
from whylogs.core.statistics.hllsketch import HllSketch
from whylogs.proto import ColumnMessage, ColumnSummary, InferredType
from whylogs.util.protobuf import message_to_dict
