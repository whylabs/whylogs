class Error(Exception):
    """Base error type for this module."""


class DeserializationError(Error):
    """Exception raised when deserializing data."""


class SerializationError(Error):
    """Exception raised when serializing data."""


class UnsupportedError(Error):
    """Exception raised when an operation is not supported."""


class LoggingError(Error):
    """Exception when an error happens during logging."""


class BadConfigError(Error):
    """Exception when an error is due to bad configuration."""
