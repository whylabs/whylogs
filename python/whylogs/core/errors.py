class Error(Exception):
    """Base error type for this module."""

    pass


class DeserializationError(Error):
    """Exception raised when deserializing data."""

    pass


class SerializationError(Error):
    """Exception raised when serializing data."""

    pass


class UnsupportedError(Error):
    """Exception raised when an operation is not supported."""

    pass
