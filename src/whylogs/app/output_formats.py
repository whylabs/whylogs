from enum import Enum, auto


class OutputFormat(Enum):
    """
    List of output formats that we support.

    * json: output as a JSON object. This is a deeply nested structure
    * csv: output as "flat" files. This will generate multiple output files
    * protobuf: output as a binary protobuf file. This is the most compact format
    """

    json = auto()
    flat = auto()
    protobuf = auto()


SUPPORTED_OUTPUT_FORMATS = list(OutputFormat.__members__.keys())
