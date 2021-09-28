package com.whylogs.cli;

import com.google.common.collect.Iterators;
import com.google.protobuf.util.JsonFormat;
import com.whylogs.cli.utils.RandomWordGenerator;
import com.whylogs.core.DatasetProfile;
import com.whylogs.core.datetime.EasyDateTimeParser;
import com.whylogs.core.message.DatasetSummaries;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.text.MessageFormat;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import lombok.Cleanup;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringEscapeUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.ArgGroup;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "profiler",
    description = "Run WhyLogs profiling against custom CSV dataset",
    mixinStandardHelpOptions = true)
public class Profiler implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(Profiler.class);

  private static final CSVFormat CSV_FORMAT =
      CSVFormat.DEFAULT.withFirstRecordAsHeader().withNullString("");

  @Option(
      names = {"-i", "--input"},
      paramLabel = "CSV_INPUT_FILE",
      description = "input csv path",
      required = true)
  File input;

  @Option(
      names = {"-o", "--output"},
      paramLabel = "JSON_OUTPUT_FILE",
      description =
          "output json file. By default the program will write to a file the same input folder using the CSV file name as a base")
  File output;

  @Option(
      names = {"-l", "--limit"},
      paramLabel = "LIMIT_NUMBER",
      description =
          "limit the number of entries to process. Can be used to quickly validate the command (default: ${DEFAULT-VALUE})")
  int limit = -1;

  @Option(
      names = {"-s", "--separator"},
      paramLabel = "SEPARATOR_CHARACTOR",
      description = "record separator. For tab character please use '\\t'")
  String delimiter = ",";

  @ArgGroup(exclusive = false)
  DateTimeColumn datetime;

  static class DateTimeColumn {
    @Option(
        names = {"-d", "--datetime"},
        description =
            "the column for parsing the datetime. If missing, we assume the dataset is running in batch mode",
        required = true)
    String column;

    @Option(
        names = {"-f", "--format"},
        description =
            "Format of the datetime column. Must specified if the datetime column is specified. "
                + "For epoch second please use 'epoch', and 'epochMillis' for epoch milliseconds",
        required = true)
    String format;
  }

  private EasyDateTimeParser dateTimeParser;
  private Path binaryOutput;

  private final Map<Instant, DatasetProfile> profiles = new ConcurrentHashMap<>();

  @SneakyThrows
  @Override
  public void run() {
    validateFiles();

    @SuppressWarnings("deprecation")
    val unescapedDelimiter = StringEscapeUtils.unescapeJava(delimiter);
    if (unescapedDelimiter.length() != 1) {
      printErrorAndExit("Separator must be 1 character only (excluding escape characters)");
    }

    val now = Instant.now();
    if (datetime != null) {
      LOG.info("Using date time format: [{}] on column: [{}]", datetime.format, datetime.column);
      this.dateTimeParser = new EasyDateTimeParser(datetime.format);
    } else {
      LOG.info("Using batch mode. Will use current time for DatasetProfile: {}", now.toString());
    }
    try {
      LOG.info("Reading input from: {}", input.getAbsolutePath());
      @Cleanup val fr = new FileReader(input);
      @Cleanup val reader = new BufferedReader(fr);
      val csvFormat = CSV_FORMAT.withDelimiter(unescapedDelimiter.charAt(0));
      @Cleanup CSVParser parser = new CSVParser(reader, csvFormat);
      val headers = parser.getHeaderMap();
      if (datetime != null) {
        if (!headers.containsKey(datetime.column)) {
          printErrorAndExit(
              "Column does not exist in the CSV header: {}. Headers: {}", datetime.column, headers);
        }
      }
      val allRecords = parser.iterator();
      if (limit > 0) {
        LOG.info("Limit stream to length: {}", limit);
      }

      val records = (limit > 0) ? Iterators.limit(allRecords, limit) : allRecords;

      // Run the tracking
      while (records.hasNext()) {
        val record = records.next();
        if (datetime != null) {
          this.parseToDateTime(headers, record);
        } else {
          this.parseBatch(now, headers, record);
        }
      }

      LOG.info(
          "Finished collecting statistics. Writing to output file: {}", output.getAbsolutePath());

      val profilesBuilder = DatasetSummaries.newBuilder();
      profiles.forEach(
          (k, profile) -> {
            final String timestamp =
                k.atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_LOCAL_DATE);
            profilesBuilder.putProfiles(timestamp, profile.toSummary());
          });

      LOG.info("Output to Protobuf binary file: {}", binaryOutput);
      try (val fos = Files.newOutputStream(binaryOutput)) {
        for (val profile : profiles.values()) {
          profile.toProtobuf().build().writeDelimitedTo(fos);
        }
      }

      try (val fileWriter = new FileWriter(output);
          val writer = new BufferedWriter(fileWriter)) {
        JsonFormat.printer().appendTo(profilesBuilder, writer);
      }

      LOG.info("Output path: {}", output.getAbsolutePath());
      LOG.info("SUCCESS");
    } catch (Exception e) {
      if (!output.delete()) {
        LOG.error("Failed to clean up output file: " + output.getAbsolutePath());
        e.printStackTrace();
      }
    }
  }

  @SneakyThrows
  private void validateFiles() {
    if (!input.exists()) {
      printErrorAndExit("ABORTING! Input file does not exist at: {}", input.getAbsolutePath());
    }
    val inputFileName = input.getName();
    val extension = FilenameUtils.getExtension(inputFileName);
    if (!"csv".equalsIgnoreCase(extension) && !"tsv".equalsIgnoreCase(extension)) {
      LOG.info("WARNING: Input does not have CSV extension. Got: {}\n", extension);
    }

    if (output == null) {
      val parentFolder = input.toPath().toAbsolutePath().getParent();
      val baseName = FilenameUtils.removeExtension(inputFileName);
      val epochMinutes = String.valueOf(Instant.now().getEpochSecond() / 60);
      val outputFileBase =
          MessageFormat.format(
              "{0}.{1}-{2}-{3}",
              baseName,
              epochMinutes,
              RandomWordGenerator.nextWord(),
              RandomWordGenerator.nextWord());
      output = parentFolder.resolve(outputFileBase + ".json").toFile();
      binaryOutput = parentFolder.resolve(outputFileBase + ".bin");
    }

    if (output.exists()) {
      printErrorAndExit("ABORTING! Output file already exists at: {}", output.getAbsolutePath());
    }

    if (!output.createNewFile()) {
      printErrorAndExit(
          "ABORTING! Failed to create new output file at: {}", output.getAbsolutePath());
    }
  }

  private void printErrorAndExit(String message, Object... args) {
    LOG.error(message, args);
    System.exit(1);
  }

  /** Switch to #stressTest if we want to battle test the memory usage further */
  private void parseToDateTime(final Map<String, Integer> headers, final CSVRecord record) {
    String issueDate = record.get(this.datetime.column);
    val time = this.dateTimeParser.parse(issueDate);
    val ds = profiles.computeIfAbsent(time, t -> new DatasetProfile(input.getName(), t));
    for (String header : headers.keySet()) {
      val idx = headers.get(header);
      val value = record.get(idx);
      ds.track(header, value);
    }
  }

  private void parseBatch(
      final Instant time, final Map<String, Integer> headers, final CSVRecord record) {
    val ds = profiles.computeIfAbsent(time, (t) -> new DatasetProfile(input.getName(), t));
    for (String header : headers.keySet()) {
      val idx = headers.get(header);
      val value = record.get(idx);
      ds.track(header, value);
    }
  }

  public static void main(String[] args) {
    new CommandLine(new Profiler()).execute(args);
  }
}
