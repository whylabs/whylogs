package com.whylogs.core.datetime;

import static org.testng.Assert.assertEquals;

import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import lombok.val;
import org.testng.annotations.Test;

public class EasyDateTimeParserTest {

  public static final ZonedDateTime ZERO_TIME =
      ZonedDateTime.of(2019, 11, 13, 7, 15, 0, 0, ZoneOffset.UTC);
  private static final Instant ZERO_INSTANT = ZERO_TIME.toInstant();
  private static final Instant UNIX_ZERO = Instant.ofEpochMilli(0);

  @Test
  public void zonedDateTime_TimeZoneInFull_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH:mm:sszzzz");
    val actual = parser.parse("2019-11-13T07:15:00-04:00");

    assertEquals(actual, ZERO_INSTANT);
  }

  @Test
  public void zonedDateTime_AbbreviatedTimeZone_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH:mm:sszzz");
    val actual = parser.parse("2019-11-13T07:15:00EDT");
    assertEquals(actual, ZERO_INSTANT);
  }

  @Test
  public void zonedDateTime_WithMilliSecond_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH:mm:ss.SSSzzz");
    val actual = parser.parse("2019-11-13T07:15:00.010EDT");
    assertEquals(actual, ZERO_INSTANT.plusMillis(10));
  }

  @Test(expectedExceptions = DateTimeParseException.class)
  public void zonedDateTime_MissingTimezoneInput_ThrowsException() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH:mm:ss.SSSzzz");
    parser.parse("2019-11-13T07:15:00");
  }

  @Test
  public void localDateTime_ValidInput_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH:mm:ss");
    val actual = parser.parse("2019-11-13T07:15:00");
    assertEquals(actual, ZERO_INSTANT);
  }

  @Test
  public void localDateTime_MissingSecond_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH:mm");
    val actual = parser.parse("2019-11-13T07:15");
    assertEquals(actual, ZERO_INSTANT);
  }

  @Test
  public void localDateTime_MissingMinute_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH");
    val actual = parser.parse("2019-11-13T07");
    assertEquals(actual, ZERO_INSTANT.minus(15, ChronoUnit.MINUTES));
  }

  @Test(expectedExceptions = DateTimeParseException.class)
  public void localDateTime_MissingYearInput_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH:mm:ss");
    parser.parse("07-16T19:20:30");
  }

  @Test
  public void localTime_FullInput_ReturnsTime() {
    val parser = new EasyDateTimeParser("HH:mm:ss");
    val actual = parser.parse("07:15:00");
    val timeForToday = LocalDate.now().atTime(7, 15).atOffset(ZoneOffset.UTC).toInstant();
    assertEquals(actual, timeForToday);
  }

  @Test
  public void localTime_MissingSecond_ReturnsTime() {
    val parser = new EasyDateTimeParser("HH:mm");
    val actual = parser.parse("07:15");
    val timeForToday = LocalDate.now().atTime(7, 15).atOffset(ZoneOffset.UTC).toInstant();
    assertEquals(actual, timeForToday);
  }

  @Test
  public void yearMonth_FullInput_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM");
    val actual = parser.parse("2019-11");
    assertEquals(actual, ZERO_TIME.truncatedTo(ChronoUnit.DAYS).withDayOfMonth(1).toInstant());
  }

  @Test
  public void yearMonth_MissingMonth_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy");
    val actual = parser.parse("2019");
    assertEquals(actual, ZERO_TIME.truncatedTo(ChronoUnit.DAYS).withDayOfYear(1).toInstant());
  }

  @Test
  public void localDate_ValidInput_ReturnsTime() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd");
    val actual = parser.parse("2019-11-13");
    assertEquals(actual, ZERO_INSTANT.truncatedTo(ChronoUnit.DAYS));
  }

  @Test
  public void localDate_AlternativeSeparator_Success() {
    val parser = new EasyDateTimeParser("yyyy/MM/dd");
    val actual = parser.parse("2019/11/13");
    assertEquals(actual, ZERO_INSTANT.truncatedTo(ChronoUnit.DAYS));
  }

  @Test
  public void epochSeconds_ValidInput_Success() {
    val parser = new EasyDateTimeParser("epoch");
    val actual = parser.parse(Long.toString(ZERO_INSTANT.getEpochSecond()));
    assertEquals(actual, ZERO_INSTANT);
  }

  @Test
  public void epochMilliseconds_ValidInput_Success() {
    val parser = new EasyDateTimeParser("epochMillis");
    val actual = parser.parse(Long.toString(ZERO_INSTANT.toEpochMilli()));
    assertEquals(actual, ZERO_INSTANT);
  }

  @Test(expectedExceptions = IllegalArgumentException.class)
  public void easyParser_InvalidFormat_ThrowException() {
    new EasyDateTimeParser("invalidFormat");
  }

  @Test
  public void esyParser_NullInput_ReturnsEpoch0() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH");
    val actual = parser.parse(null);
    assertEquals(actual, UNIX_ZERO);
  }

  @Test
  public void esyParser_EmptyInput_ReturnsEpoch0() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH");
    val actual = parser.parse("");
    assertEquals(actual, UNIX_ZERO);
  }

  @Test
  public void esyParser_NullStringInput_ReturnsEpoch0() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH");
    val actual = parser.parse("NULL");
    assertEquals(actual, UNIX_ZERO);
  }

  @Test
  public void esyParser_NaNStringInput_ReturnsEpoch0() {
    val parser = new EasyDateTimeParser("yyyy-MM-dd'T'HH");
    val actual = parser.parse("NaN");
    assertEquals(actual, UNIX_ZERO);
  }
}
