package com.whylogs.core.utils;

import com.google.protobuf.ByteString;
import com.whylogs.core.message.DatasetSummary;
import java.time.Instant;
import java.util.stream.Collectors;
import lombok.experimental.UtilityClass;
import lombok.val;

@SuppressWarnings("unused")
@UtilityClass
public class ProtobufHelper {
  public String summaryToString(DatasetSummary summary) {
    val name = summary.getProperties().getSessionId();
    val tags =
        summary.getProperties().getTagsList().asByteStringList().stream()
            .map(ByteString::toStringUtf8)
            .collect(Collectors.joining(","));
    val timestamp = Instant.ofEpochMilli(summary.getProperties().getSessionTimestamp()).toString();
    val columns = summary.getColumnsMap().keySet();

    return String.format(
        "Name: %s. Tags: %s. Timestamp: %s. Columns: %s", name, tags, timestamp, columns);
  }
}
