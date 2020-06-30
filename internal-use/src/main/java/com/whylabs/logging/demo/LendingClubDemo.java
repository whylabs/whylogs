package com.whylabs.logging.demo;

import com.google.protobuf.util.JsonFormat;
import com.whylabs.logging.core.DatasetProfile;
import com.whylabs.logging.core.datetime.EasyDateTimeParser;
import com.whylabs.logging.core.message.DatasetSummaries;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Locale;
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

@SuppressWarnings("Duplicates")
public class LendingClubDemo {
  private static final Scanner scanner = new Scanner(System.in);

  private static final Map<Instant, DatasetProfile> profiles = new HashMap<>();
  private static final DateTimeFormatter dateTimeFormatter =
      //      DateTimeFormatter.ofPattern("yyy-MM-dd");
      DateTimeFormatter.ofPattern("MMM-yyyy").withLocale(Locale.ENGLISH);
  public static final String INPUT = "lendingclub_accepted_2007_to_2017.csv";

  public static void main(String[] args) throws Exception {
    val dateTimeParser = new EasyDateTimeParser("MMM-yyyy");
    printAndWait("Current process ID: " + ManagementFactory.getRuntimeMXBean().getName());

    @Cleanup val fis = new FileInputStream("/Users/andy/Downloads/reserach_data/" + INPUT);
    @Cleanup val reader = new InputStreamReader(fis);
    CSVFormat format = CSVFormat.DEFAULT.withFirstRecordAsHeader().withNullString("");
    @Cleanup CSVParser parser = new CSVParser(reader, format);
    val spliterator = Spliterators.spliteratorUnknownSize(parser.iterator(), 0);
    StreamSupport.stream(spliterator, false)
        .limit(100000)
        .iterator()
        .forEachRemaining(record -> normalTracking(record, dateTimeParser, "issue_d"));

    val profilesBuilder = DatasetSummaries.newBuilder();
    profiles.forEach(
        (k, profile) -> {
          final String timestamp =
              k.atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE);
          profilesBuilder.putProfiles(timestamp, profile.toSummary());
        });

    String output = "/Users/andy/Downloads/reserach_data/lendingclub_accepted.json";
    try (val writer = new FileWriter(output)) {
      JsonFormat.printer().appendTo(profilesBuilder, writer);
    }
    printAndWait("Finished writing to file. Enter anything to exit");
  }

  /** Switch to #stressTest if we want to battle test the memory usage further */
  private static void normalTracking(
      CSVRecord record, EasyDateTimeParser dateTimeParser, String dateTimeColumn) {
    String issueDate = record.get(dateTimeColumn);
    val instant = dateTimeParser.parse(issueDate);
    profiles.compute(
        instant,
        (time, datasetProfile) -> {
          if (datasetProfile == null) {
            datasetProfile = new DatasetProfile(INPUT, time);
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
