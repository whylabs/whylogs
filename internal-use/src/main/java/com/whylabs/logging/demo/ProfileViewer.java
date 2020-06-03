package com.whylabs.logging.demo;

import com.google.protobuf.util.JsonFormat;
import com.whylabs.logging.core.DatasetProfile;
import com.whylabs.logging.core.data.DatasetSummaries;
import com.whylabs.logging.core.format.DatasetProfileMessage;
import java.io.BufferedInputStream;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

@Command(
    name = "viewer",
    description = "Serialized WhyLogs binary output to JSON",
    mixinStandardHelpOptions = true)
public class ProfileViewer implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(ProfileViewer.class);

  @Option(
      names = {"-i", "--input"},
      paramLabel = "BINARY_OUTPUT",
      description = "binary output of WhyLogs Profiler",
      required = true)
  File input;

  @Option(
      names = {"-o", "--output"},
      paramLabel = "JSON_OUTPUT_FILE",
      description =
          "output json file. By default the program will write to a file the same input folder using the CSV file name as a base")
  File output;

  @SneakyThrows
  private void validateFiles() {
    if (!input.exists()) {
      LOG.error("ABORTING! Input file does not exist at: {}", input.getAbsolutePath());
      System.exit(1);
    }
    val inputFileName = input.getName();
    val extension = FilenameUtils.getExtension(inputFileName);
    if (!"bin".equalsIgnoreCase(extension)) {
      LOG.info("WARNING: Input does not have bin extension. Got: {}\n", extension);
    }

    if (output == null) {
      val parentFolder = input.toPath().toAbsolutePath().getParent();
      val baseName = FilenameUtils.removeExtension(inputFileName);
      output = parentFolder.resolve(baseName + ".json").toFile();
    }

    if (output.exists()) {
      LOG.error("ABORTING! Output file already exists at: {}", output.getAbsolutePath());
      System.exit(1);
    }

    if (!output.createNewFile()) {
      LOG.error("ABORTING! Failed to create new output file at: {}", output.getAbsolutePath());
      System.exit(1);
    }
  }

  @SneakyThrows
  @Override
  public void run() {
    validateFiles();

    val profileSummariesBuilder = DatasetSummaries.newBuilder();

    try (val bis = new BufferedInputStream(new FileInputStream(input))) {
      while (bis.available() > 0) {
        final DatasetProfileMessage message = DatasetProfileMessage.parseDelimitedFrom(bis);

        val ds = DatasetProfile.fromProtobuf(message);
        final String timestamp =
            ds.getSessionTimestamp()
                .atZone(ZoneOffset.UTC)
                .format(DateTimeFormatter.ISO_LOCAL_DATE);

        profileSummariesBuilder.putProfiles(timestamp, ds.toSummary());
      }
    }

    try (val fos = Files.newOutputStream(output.toPath());
        val writer = new BufferedWriter(new OutputStreamWriter(fos))) {
      JsonFormat.printer().appendTo(profileSummariesBuilder.build(), writer);
    }
  }
}
