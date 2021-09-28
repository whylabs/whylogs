package com.whylogs.core.datetime;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.MonthDay;
import java.time.Year;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.TemporalAccessor;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.val;

class DateTimeFormatParser<TIME_FORMAT extends TemporalAccessor> {

  private final Class<TIME_FORMAT> timeFormat;
  private final BiFunction<DateTimeFormatter, String, Instant> function;

  public DateTimeFormatParser(
      Class<TIME_FORMAT> timeFormat, BiFunction<DateTimeFormatter, String, Instant> function) {
    this.timeFormat = timeFormat;
    this.function = function;
  }

  public Class<TIME_FORMAT> getTimeFormatClass() {
    return timeFormat;
  }

  public Instant parse(DateTimeFormatter formatter, String input) {
    return function.apply(formatter, input);
  }

  static final DateTimeFormatParser<ZonedDateTime> ZONED_DATETIME =
      new DateTimeFormatParser<>(
          ZonedDateTime.class,
          (dateTimeFormatter, input) -> {
            val dateTime = ZonedDateTime.parse(input, dateTimeFormatter);
            return dateTime.toInstant();
          });

  static final DateTimeFormatParser<LocalDateTime> LOCAL_DATETIME =
      new DateTimeFormatParser<>(
          LocalDateTime.class,
          (dateTimeFormatter, input) -> {
            val dateTime = LocalDateTime.parse(input, dateTimeFormatter);
            return dateTime.atZone(ZoneOffset.UTC).toInstant();
          });

  static final DateTimeFormatParser<LocalTime> LOCAL_TIME =
      new DateTimeFormatParser<>(
          LocalTime.class,
          (dateTimeFormatter, input) -> {
            val dateTime = LocalTime.parse(input, dateTimeFormatter);
            return dateTime.atDate(LocalDate.now()).atZone(ZoneOffset.UTC).toInstant();
          });

  static final DateTimeFormatParser<MonthDay> MONTH_DAY =
      new DateTimeFormatParser<>(
          MonthDay.class,
          (dateTimeFormatter, input) -> {
            val dateTime = MonthDay.parse(input, dateTimeFormatter);
            val currentYear = Year.now().getValue();
            return dateTime.atYear(currentYear).atStartOfDay().atZone(ZoneOffset.UTC).toInstant();
          });

  static final DateTimeFormatParser<LocalDate> LOCAL_DATE =
      new DateTimeFormatParser<>(
          LocalDate.class,
          (dateTimeFormatter, input) -> {
            val dateTime = LocalDate.parse(input, dateTimeFormatter);
            return dateTime.atStartOfDay().atZone(ZoneOffset.UTC).toInstant();
          });

  static final DateTimeFormatParser<YearMonth> YEAR_MONTH =
      new DateTimeFormatParser<>(
          YearMonth.class,
          (dateTimeFormatter, input) -> {
            val dateTime = YearMonth.parse(input, dateTimeFormatter);
            return dateTime.atDay(1).atStartOfDay().atZone(ZoneOffset.UTC).toInstant();
          });

  static final DateTimeFormatParser<Year> YEAR =
      new DateTimeFormatParser<>(
          Year.class,
          (dateTimeFormatter, input) -> {
            val dateTime = Year.parse(input, dateTimeFormatter);
            return dateTime.atMonth(1).atDay(1).atStartOfDay().atZone(ZoneOffset.UTC).toInstant();
          });

  static final DateTimeFormatParser<Instant> EPOCH_SECONDS =
      new DateTimeFormatParser<>(
          Instant.class,
          (dateTimeFormatter, input) -> {
            try {
              return Instant.ofEpochSecond(Long.parseLong(input));
            } catch (NumberFormatException e) {
              throw new DateTimeParseException("Invalid number format", input, 0, e);
            }
          });

  static final DateTimeFormatParser<Instant> EPOCH_MILLISECONDS =
      new DateTimeFormatParser<>(
          Instant.class,
          (dateTimeFormatter, input) -> {
            try {
              return Instant.ofEpochMilli(Long.parseLong(input));
            } catch (NumberFormatException e) {
              throw new DateTimeParseException("Invalid number format", input, 0, e);
            }
          });
  static final Set<Class<?>> SUPPORT_TIME_CLASSES =
      Stream.of(
              ZONED_DATETIME,
              LOCAL_DATE,
              LOCAL_TIME,
              MONTH_DAY,
              LOCAL_DATE,
              YEAR_MONTH,
              YEAR,
              EPOCH_SECONDS,
              EPOCH_MILLISECONDS)
          .map(DateTimeFormatParser::getTimeFormatClass)
          .collect(Collectors.toSet());
}
