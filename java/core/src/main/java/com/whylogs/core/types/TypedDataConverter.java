package com.whylogs.core.types;

import static java.util.stream.Collectors.toSet;

import com.whylogs.core.message.InferredType;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import lombok.val;

@UtilityClass
public class TypedDataConverter {
  public static final Set<InferredType.Type> NUMERIC_TYPES =
      Stream.of(InferredType.Type.FRACTIONAL, InferredType.Type.INTEGRAL).collect(toSet());

  private final Pattern FRACTIONAL = Pattern.compile("^[-+]? ?\\d+[.]\\d+$");
  private final Pattern INTEGRAL = Pattern.compile("^[-+]? ?\\d+$");
  private final Pattern BOOLEAN = Pattern.compile("^(?i)true|false$");
  private final Pattern EMPTY_SPACES = Pattern.compile("\\s");

  private final ThreadLocal<Matcher> FRACTIONAL_MATCHER =
      ThreadLocal.withInitial(() -> FRACTIONAL.matcher(""));
  private final ThreadLocal<Matcher> INTEGRAL_MATCHER =
      ThreadLocal.withInitial(() -> INTEGRAL.matcher(""));
  private final ThreadLocal<Matcher> BOOLEAN_MATCHER =
      ThreadLocal.withInitial(() -> BOOLEAN.matcher(""));
  private final ThreadLocal<Matcher> EMPTY_SPACES_REMOVER =
      ThreadLocal.withInitial(() -> EMPTY_SPACES.matcher(""));

  public TypedData convert(Object data) {
    if (data == null) {
      return null;
    }

    if (data instanceof TypedData) {
      return (TypedData) data;
    }

    if (data instanceof String) {
      return getTypedStringData((String) data);
    }

    if (data instanceof Double || data instanceof Float) {
      final double doubleValue = ((Number) data).doubleValue();
      return TypedData.fractionalValue(doubleValue);
    }

    if (data instanceof Integer || data instanceof Long || data instanceof Short) {
      final long longValue = ((Number) data).longValue();
      return TypedData.integralValue(longValue);
    }

    if (data instanceof Boolean) {
      return TypedData.booleanValue((boolean) data);
    }

    return TypedData.unknownValue();
  }

  private TypedData getTypedStringData(String data) {
    if (System.getenv().get("WHYLOGS_ENABLE_STRING_MATCHING") != null) {
      INTEGRAL_MATCHER.get().reset(data);
      if (INTEGRAL_MATCHER.get().matches()) {
        val trimmedText = EMPTY_SPACES_REMOVER.get().reset(data).replaceAll("");
        return TypedData.integralValue(Long.parseLong(trimmedText));
      }

      FRACTIONAL_MATCHER.get().reset(data);
      if (FRACTIONAL_MATCHER.get().matches()) {
        val trimmedText = EMPTY_SPACES_REMOVER.get().reset(data).replaceAll("");
        return TypedData.fractionalValue(Double.parseDouble(trimmedText));
      }

      BOOLEAN_MATCHER.get().reset(data);
      if (BOOLEAN_MATCHER.get().matches()) {
        val trimmedText = EMPTY_SPACES_REMOVER.get().reset(data).replaceAll("");
        return TypedData.booleanValue(Boolean.parseBoolean(trimmedText));
      }
    }

    return TypedData.stringValue(data);
  }
}
