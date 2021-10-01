package com.whylogs.core.types;

import com.whylogs.core.message.InferredType;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NonNull;
import lombok.Value;

@Value
@Builder(setterPrefix = "set", access = AccessLevel.PRIVATE)
public class TypedData {
  private static final TypedData UNKNOWN_INSTANCE =
      TypedData.builder().setType(InferredType.Type.UNKNOWN).build();
  private static final TypedData BOOLEAN_TRUE_INSTANCE =
      TypedData.builder().setType(InferredType.Type.BOOLEAN).setBooleanValue(true).build();
  private static final TypedData BOOLEAN_FALSE_INSTANCE =
      TypedData.builder().setType(InferredType.Type.BOOLEAN).setBooleanValue(false).build();

  @NonNull InferredType.Type type;
  boolean booleanValue;
  long integralValue;
  double fractional;
  String stringValue;

  public static TypedData booleanValue(boolean booleanValue) {
    if (booleanValue) {
      return BOOLEAN_TRUE_INSTANCE;
    } else {
      return BOOLEAN_FALSE_INSTANCE;
    }
  }

  public static TypedData stringValue(String stringValue) {
    return TypedData.builder()
        .setType(InferredType.Type.STRING)
        .setStringValue(stringValue)
        .build();
  }

  public static TypedData integralValue(long integralValue) {
    return TypedData.builder()
        .setType(InferredType.Type.INTEGRAL)
        .setIntegralValue(integralValue)
        .build();
  }

  public static TypedData fractionalValue(double fractionalValue) {
    return TypedData.builder()
        .setType(InferredType.Type.FRACTIONAL)
        .setFractional(fractionalValue)
        .build();
  }

  public static TypedData unknownValue() {
    // we don't track "unknown" since we can't track any statistics
    return UNKNOWN_INSTANCE;
  }
}
