package com.whylabs.logging.firehose.iterator;

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.whylabs.logging.core.format.MessageSegment;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import lombok.val;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FirehoseRecordGrouper implements Iterator<List<Record>> {
  private static final Logger LOG = LoggerFactory.getLogger(FirehoseRecordGrouper.class);

  private static final int MAX_SIZE_IN_BYTES = 4_000_000; // 4MB for a batch put message

  private final int maxLen;
  private final Iterator<MessageSegment> segments;
  private final List<Record> records;
  private final ByteArrayOutputStream outputStream;

  private int currentMessageSize;

  FirehoseRecordGrouper(int maxLen, Iterator<MessageSegment> segments) {
    this.outputStream = new ByteArrayOutputStream();
    this.maxLen = maxLen;
    this.segments = segments;
    this.records = new ArrayList<>();
    this.currentMessageSize = 0;
  }

  public FirehoseRecordGrouper(Iterator<MessageSegment> segments) {
    this(MAX_SIZE_IN_BYTES, segments);
  }

  @Override
  public boolean hasNext() {
    return records.size() > 0 || segments.hasNext();
  }

  @Override
  public List<Record> next() {
    while (segments.hasNext()) {
      val chunkMessage = segments.next();
      outputStream.reset();

      try {
        chunkMessage.writeDelimitedTo(outputStream);
      } catch (IOException e) {
        LOG.warn("Failed to deserialize message to byte array. Skipping the record", e);
        continue;
      }

      val bytes = outputStream.toByteArray();
      val byteBuffer = ByteBuffer.wrap(bytes);
      val request = new Record().withData(byteBuffer);

      if (currentMessageSize + bytes.length <= maxLen) {
        records.add(request);
        currentMessageSize += bytes.length;
      } else {
        val result = new ArrayList<>(this.records);
        records.clear();

        records.add(request);
        currentMessageSize = bytes.length;

        return Collections.unmodifiableList(result);
      }
    }

    if (this.records.isEmpty()) {
      throw new NoSuchElementException();
    }

    val result = new ArrayList<>(this.records);
    records.clear();
    currentMessageSize = 0;
    return Collections.unmodifiableList(result);
  }
}
