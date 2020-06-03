package com.whylabs.logging.core.datetime;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoField;
import java.util.Locale;
import lombok.val;

public class EasyDateTimeParser {

  public static final String EPOCH_SECONDS_FORMAT = "epoch";
  public static final String EPOCH_MILLIS_FORMAT = "epochMillis";
  public static final Instant BEGINNING_OF_TIME = Instant.ofEpochMilli(0);

  private final DateTimeFormatter dateTimeFormatter;
  private volatile DateTimeFormatParser<?> parser;

  public EasyDateTimeParser(String format) {
    if (EPOCH_SECONDS_FORMAT.equalsIgnoreCase(format)) {
      this.dateTimeFormatter = null;
      this.parser = DateTimeFormatParser.EPOCH_SECONDS;
    } else if (EPOCH_MILLIS_FORMAT.equalsIgnoreCase(format)) {
      this.dateTimeFormatter = null;
      this.parser = DateTimeFormatParser.EPOCH_MILLISECONDS;
    } else {
      this.dateTimeFormatter = DateTimeFormatter.ofPattern(format).withLocale(Locale.ENGLISH);
    }
  }

  public Instant parse(String input) {
    if (input == null
        || "nan".equalsIgnoreCase(input)
        || "null".equalsIgnoreCase(input)
        || "".equalsIgnoreCase(input)) {
      return BEGINNING_OF_TIME;
    }

    if (this.parser == null) {
      return this.calculateFormat(input);
    } else {
      return this.parser.parse(dateTimeFormatter, input);
    }
  }

  private Instant calculateFormat(String firstInput) {
    val parsed = dateTimeFormatter.parse(firstInput);
    val hasYear = parsed.isSupported(ChronoField.YEAR);
    val hasMonth = parsed.isSupported(ChronoField.MONTH_OF_YEAR);
    val hasDayOfMonth = parsed.isSupported(ChronoField.DAY_OF_MONTH);
    val hasHourOfDay = parsed.isSupported(ChronoField.HOUR_OF_DAY);

    if (hasHourOfDay) {
      if (hasYear && hasMonth && hasDayOfMonth) {
        if (dateTimeFormatter.getZone() != null) {
          this.parser = DateTimeFormatParser.ZONED_DATETIME;
        } else {
          this.parser = DateTimeFormatParser.LOCAL_DATETIME;
        }
      } else if (!hasYear && !hasMonth && !hasDayOfMonth) {
        this.parser = DateTimeFormatParser.LOCAL_TIME;
      }
    } else if (hasYear && hasMonth & hasDayOfMonth) {
      this.parser = DateTimeFormatParser.LOCAL_DATE;
    } else if (!hasYear && hasMonth && hasDayOfMonth) {
      this.parser = DateTimeFormatParser.MONTH_DAY;
    } else if (hasYear && hasMonth) {
      this.parser = DateTimeFormatParser.YEAR_MONTH;
    } else if (hasYear) {
      this.parser = DateTimeFormatParser.YEAR;
    }
    if (this.parser == null) {
      throw new RuntimeException(
          "Failed to match supported format. Supported format: "
              + DateTimeFormatParser.SUPPORT_TIME_CLASSES);
    }
    try {
      return this.parse(firstInput);
    } catch (Exception e) {
      throw new RuntimeException(
          "The determined formatter failed to parse the input. Supported format: "
              + DateTimeFormatParser.SUPPORT_TIME_CLASSES,
          e);
    }
  }
}
