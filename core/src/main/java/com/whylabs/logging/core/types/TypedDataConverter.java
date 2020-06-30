package com.whylabs.logging.core.types;

import static java.util.stream.Collectors.toSet;

import com.whylabs.logging.core.message.InferredType.Type;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import lombok.experimental.UtilityClass;
import lombok.val;

@UtilityClass
public class TypedDataConverter {
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

  private final Set<Type> NUMERIC_TYPES =
      Stream.of(Type.FRACTIONAL, Type.INTEGRAL).collect(toSet());

  public TypedData convert(Object data) {
    if (data == null) {
      return null;
    }

    if (data instanceof String) {
      val strData = (String) data;

      INTEGRAL_MATCHER.get().reset(strData);
      if (INTEGRAL_MATCHER.get().matches()) {
        val trimmedText = EMPTY_SPACES_REMOVER.get().reset(strData).replaceAll("");
        return TypedData.integralValue(Long.parseLong(trimmedText));
      }

      FRACTIONAL_MATCHER.get().reset(strData);
      if (FRACTIONAL_MATCHER.get().matches()) {
        val trimmedText = EMPTY_SPACES_REMOVER.get().reset(strData).replaceAll("");
        return TypedData.fractionalValue(Double.parseDouble(trimmedText));
      }

      BOOLEAN_MATCHER.get().reset(strData);
      if (BOOLEAN_MATCHER.get().matches()) {
        val trimmedText = EMPTY_SPACES_REMOVER.get().reset(strData).replaceAll("");
        return TypedData.booleanValue(Boolean.parseBoolean(trimmedText));
      }

      return TypedData.stringValue(strData);
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
}
