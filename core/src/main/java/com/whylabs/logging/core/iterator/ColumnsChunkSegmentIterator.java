package com.whylabs.logging.core.iterator;

import com.whylabs.logging.core.format.ColumnMessage;
import com.whylabs.logging.core.format.ColumnsChunkSegment;
import java.util.Iterator;
import java.util.NoSuchElementException;
import lombok.val;

public class ColumnsChunkSegmentIterator implements Iterator<ColumnsChunkSegment> {

  private static final int MAX_LEN_IN_BYTES = 1_000_000 - 10; // 1MB for each message

  private final int maxChunkLength;
  private final Iterator<ColumnMessage> iterator;
  private final ColumnsChunkSegment.Builder builder;

  private int contentLength;
  private int numberOfColumns;

  public ColumnsChunkSegmentIterator(Iterator<ColumnMessage> iterator, String marker) {
    this(MAX_LEN_IN_BYTES, iterator, marker);
  }

  ColumnsChunkSegmentIterator(int maxChunkLength, Iterator<ColumnMessage> iterator, String marker) {
    this.maxChunkLength = maxChunkLength;
    this.iterator = iterator;
    this.builder = ColumnsChunkSegment.newBuilder().setMarker(marker);
    this.contentLength = 0;
    this.numberOfColumns = 0;
  }

  @Override
  public boolean hasNext() {
    if (this.numberOfColumns > 0) {
      return true;
    }

    return iterator.hasNext();
  }

  @Override
  public ColumnsChunkSegment next() {
    while (iterator.hasNext()) {
      final ColumnMessage columnMessage = iterator.next();

      // TODO: handle the case if messageLen > baselength
      val messageLen = columnMessage.getSerializedSize();
      val candidateContentSize = contentLength + messageLen;
      val canItemBeAppended = candidateContentSize <= maxChunkLength;
      if (canItemBeAppended) {
        builder.addColumns(columnMessage);
        this.numberOfColumns++;
        contentLength = candidateContentSize;
      } else {
        val result = builder.build();
        builder.clearColumns();
        builder.addColumns(columnMessage);
        this.numberOfColumns = 1;
        contentLength = messageLen;

        return result;
      }
    }

    if (this.numberOfColumns > 0) {
      val result = builder.build();
      this.builder.clearColumns();
      this.contentLength = 0;
      this.numberOfColumns = 0;
      return result;
    }

    throw new NoSuchElementException();
  }
}
