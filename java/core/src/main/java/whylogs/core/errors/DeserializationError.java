package whylogs.core.errors;

/** Exception raised when deserializing data. */
public class DeserializationError extends Error {
  public DeserializationError(String message) {
    super(message);
  }
}
