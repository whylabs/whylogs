package com.whylabs.logging.demo;

import static java.time.temporal.ChronoField.DAY_OF_MONTH;
import static java.time.temporal.ChronoField.MONTH_OF_YEAR;
import static java.time.temporal.ChronoField.YEAR;

import com.whylabs.logging.core.DatasetProfile;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.SignStyle;
import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

@SuppressWarnings("DuplicatedCode")
public class ProfilerDemo {
  private static final Scanner scanner = new Scanner(System.in);

  private static Map<Instant, DatasetProfile> profiles = new HashMap<>();
  private static DateTimeFormatter dateTimeFormatter =
      new DateTimeFormatterBuilder()
          .appendValue(MONTH_OF_YEAR, 2)
          .appendLiteral('/')
          .appendValue(DAY_OF_MONTH, 2)
          .appendLiteral('/')
          .appendValue(YEAR, 4, 10, SignStyle.EXCEEDS_PAD)
          .toFormatter();

  public static void main(String[] args) throws Exception {
    val profile = new DatasetProfile("data", Instant.now());

    printAndWait("Current process ID: " + ManagementFactory.getRuntimeMXBean().getName());

    @Cleanup
    val fis =
        new FileInputStream(
            "/Users/andy/Downloads/reserach_data/Parking_Violations_Issued_-_Fiscal_Year_2017.csv");
    @Cleanup val reader = new InputStreamReader(fis);
    CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withNullString("");
    @Cleanup CSVParser parser = new CSVParser(reader, format);
    val spliterator = Spliterators.spliteratorUnknownSize(parser.iterator(), 0);
    StreamSupport.stream(spliterator, false)
        //        .limit(10)
        .iterator()
        .forEachRemaining(ProfilerDemo::normalTracking);
    try (val writer =
        new FileWriter("/Users/andy/Downloads/reserach_data/nydata_summarized.json")) {
      val interpretableDatasetProfileMap =
          profiles.entrySet().stream()
              .collect(
                  Collectors.toMap(
                      e ->
                          e.getKey()
                              .atZone(ZoneOffset.UTC)
                              .format(DateTimeFormatter.ISO_LOCAL_DATE),
                      e -> e.getValue().toSummary()));
    }
    printAndWait("Finished writing to file. Enter anything to exit");
  }

  /** Switch to #stressTest if we want to battle test the memory usage further */
  private static void normalTracking(CSVRecord record) {
    val instant =
        LocalDate.parse(record.get("Issue Date"), dateTimeFormatter)
            .atStartOfDay()
            .atZone(ZoneOffset.UTC)
            .toInstant();
    profiles.compute(
        instant,
        (time, datasetProfile) -> {
          if (datasetProfile == null) {
            datasetProfile =
                new DatasetProfile("Parking_Violations_Issued_-_Fiscal_Year_2017", time);
          }

          datasetProfile.track(record.toMap());
          return datasetProfile;
        });
  }

  private static void stressTest(DatasetProfile profile, CSVRecord record) {
    for (int i = 0; i < 10; i++) {
      int finalI = i;
      val modifiedMap =
          record.toMap().entrySet().stream()
              .collect(Collectors.toMap(e -> e.getKey() + finalI, e -> e.getValue() + finalI));

      profile.track(modifiedMap);
    }
  }

  @SneakyThrows
  private static void printAndWait(String message) {
    System.out.print(message + ": ");

    //    String input = scanner.next();
    //    System.out.println("Got input: " + input);
    System.out.println();
    System.out.flush();
  }
}
