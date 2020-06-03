package com.whylabs.logging.core;

import com.google.common.base.Preconditions;
import lombok.experimental.UtilityClass;

@UtilityClass
public class SchemaInformation {
  final int SCHEMA_MAJOR_VERSION = 1;
  final int SCHEMA_MINOR_VERSION = 0;

  void validateSchema(int majorVersion, int minorVersion) {
    Preconditions.checkArgument(
        SCHEMA_MAJOR_VERSION == majorVersion,
        "Expect major version %s, got %s",
        SCHEMA_MAJOR_VERSION,
        minorVersion);
    Preconditions.checkArgument(
        SCHEMA_MINOR_VERSION == minorVersion,
        "Expect minor version %s, got %s",
        SCHEMA_MINOR_VERSION,
        minorVersion);
  }
}
