package com.whylabs.logging.core.iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.whylabs.logging.core.message.ColumnMessage;
import com.whylabs.logging.core.message.ColumnsChunkSegment;
import java.util.Collections;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

public class ColumnsChunkSegmentIteratorTest {
  @Test
  public void chunkIterator_SingleMessage_OutputChunk() {
    val column = ColumnMessage.newBuilder().setName("RandomColumn").build();
    val singleIt = Iterators.singletonIterator(column);
    val chunks = Lists.newArrayList(new ColumnsChunkSegmentIterator(singleIt, "marker"));
    assertThat(chunks, hasSize(1));
    assertThat(chunks.get(0).getColumnsCount(), is(1));
  }

  @Test
  public void chunkIterator_SingleEmptyMessage_OutputChunk() {
    val column = ColumnMessage.newBuilder().build();
    val singleIt = Iterators.singletonIterator(column);
    val chunks = Lists.newArrayList(new ColumnsChunkSegmentIterator(singleIt, "marker"));
    assertThat(chunks, hasSize(1));
    assertThat(chunks.get(0).getColumnsCount(), is(1));
  }

  @Test
  public void chunkIterator_ThousandColumnMessages_OutputCorrectChunks() {
    // our configuration
    val maxChunkLen = 100_000;
    val numberOfColumns = 12345;
    val columnNameLen = 2_345;

    val colBuilder =
        ColumnMessage.newBuilder().setName(RandomStringUtils.randomAlphabetic(columnNameLen));
    val baseMessageSize = colBuilder.build().getSerializedSize();
    val maxMessagesPerChunk = maxChunkLen / baseMessageSize;

    val it =
        IntStream.range(0, numberOfColumns).boxed().map(ignored -> colBuilder.build()).iterator();

    val chunks = Lists.newArrayList(new ColumnsChunkSegmentIterator(maxChunkLen, it, "marker"));
    assertThat(chunks, hasSize(lessThanOrEqualTo(numberOfColumns / maxMessagesPerChunk + 1)));

    val chunkSizes =
        chunks.stream().map(ColumnsChunkSegment::getSerializedSize).collect(Collectors.toList());
    assertThat(chunkSizes, everyItem(lessThanOrEqualTo(maxChunkLen)));

    val totalColumnCount = chunks.stream().mapToInt(ColumnsChunkSegment::getColumnsCount).sum();
    assertThat(totalColumnCount, is(numberOfColumns));
  }

  @Test
  public void chunkIterator_ThousandEmptyColumnMessages_OutputCorrectChunks() {
    val maxChunkLen = 100_000;
    val numberOfColumns = 12345;

    val emptyColBuilder = ColumnMessage.newBuilder();

    val it =
        IntStream.range(0, numberOfColumns)
            .boxed()
            .map(ignored -> emptyColBuilder.build())
            .iterator();

    val chunks = Lists.newArrayList(new ColumnsChunkSegmentIterator(maxChunkLen, it, "marker"));
    assertThat(chunks, hasSize(1));

    val totalColumnCount = chunks.stream().mapToInt(ColumnsChunkSegment::getColumnsCount).sum();
    assertThat(totalColumnCount, is(numberOfColumns));
  }

  @Test
  public void chunkIterator_EmptyInput_OutputIsEmpty() {
    val chunks =
        Lists.newArrayList(new ColumnsChunkSegmentIterator(Collections.emptyIterator(), ""));
    assertThat(chunks, hasSize(0));
  }
}
