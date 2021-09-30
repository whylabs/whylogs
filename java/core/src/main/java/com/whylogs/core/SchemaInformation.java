package com.whylogs.core;

import com.google.common.base.Preconditions;
import lombok.experimental.UtilityClass;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@UtilityClass
public class SchemaInformation {
  final int SCHEMA_MAJOR_VERSION = 1;
  final int SCHEMA_MINOR_VERSION = 3;

  void validateSchema(int majorVersion, int minorVersion) {
    Preconditions.checkArgument(
        SCHEMA_MAJOR_VERSION == majorVersion,
        String.format("Expect major version %s, got %s", SCHEMA_MAJOR_VERSION, majorVersion));
    Preconditions.checkArgument(
        SCHEMA_MINOR_VERSION >= minorVersion,
        String.format(
            "Does not support forward compatibility. Minor version: %s, got: %s",
            SCHEMA_MINOR_VERSION, minorVersion));
    if (SCHEMA_MINOR_VERSION > minorVersion) {
      log.warn("Expect minor version {}. Got: {}", SCHEMA_MINOR_VERSION, minorVersion);
    }
  }
}
