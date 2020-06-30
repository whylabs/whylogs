package com.whylabs.logging.firehose.iterator;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

import com.amazonaws.services.kinesisfirehose.model.Record;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.whylabs.logging.core.message.DatasetMetadataSegment;
import com.whylabs.logging.core.message.DatasetProperties;
import com.whylabs.logging.core.message.MessageSegment;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import lombok.val;
import org.apache.commons.lang3.RandomStringUtils;
import org.testng.annotations.Test;

public class FirehoseRecordGrouperTest {

  @Test
  public void grouper_EmptyInput_EmptyOutput() {
    val grouper = new FirehoseRecordGrouper(Collections.emptyIterator());
    final val result = Lists.newArrayList(grouper);
    assertThat(result, is(empty()));
  }

  @Test
  public void chunkIterator_SingleEmptyMessage_OutputChunk() {
    val emptySegment = MessageSegment.newBuilder().build();
    val singleIt = Iterators.singletonIterator(emptySegment);
    val groups = Lists.newArrayList(new FirehoseRecordGrouper(singleIt));
    assertThat(groups, hasSize(1));
    assertThat(groups.get(0).size(), is(1));
  }

  @Test
  public void grouper_LargeNumberOfMessages_OutputCorrectGrouping() {
    // our configuration
    val maxChunkLen = 4_000_000;
    val noOfMessages = 99;
    val sessionIdLen = 9_00_000; // 900kb for each message

    val props =
        DatasetProperties.newBuilder()
            .setSessionId(RandomStringUtils.randomAlphabetic(sessionIdLen));
    val messageSegmentBuilder =
        MessageSegment.newBuilder()
            .setMetadata(DatasetMetadataSegment.newBuilder().setProperties(props));
    val baseMessageSize = messageSegmentBuilder.build().getSerializedSize();
    val maxMessagesPerChunk = maxChunkLen / baseMessageSize;

    val segments =
        IntStream.range(0, noOfMessages)
            .boxed()
            .map(ignored -> messageSegmentBuilder.build())
            .iterator();

    val groups = Lists.newArrayList(new FirehoseRecordGrouper(maxChunkLen, segments));
    assertThat(groups, hasSize(lessThanOrEqualTo(noOfMessages / maxMessagesPerChunk + 1)));

    val recordGroupSizes =
        groups.stream()
            .map(list -> list.stream().mapToInt(record -> record.getData().array().length).sum())
            .collect(Collectors.toList());
    assertThat(recordGroupSizes, everyItem(lessThanOrEqualTo(maxChunkLen)));

    val totalRecordCount = groups.stream().mapToInt(List::size).sum();
    assertThat(totalRecordCount, is(noOfMessages));
  }

  @Test
  public void grouper_EmptyMessagesInLargeNumer_OutputSingleGrouping() {
    // our configuration
    val maxChunkLen = 4_000_000;
    val noOfMessages = 9999;

    // empty segments
    val messageSegment = MessageSegment.newBuilder().build();

    val segments =
        IntStream.range(0, noOfMessages).boxed().map(ignored -> messageSegment).iterator();

    val groups = Lists.newArrayList(new FirehoseRecordGrouper(maxChunkLen, segments));
    assertThat(groups, hasSize(1));

    // we write out the length of the message, which is 1 byte so total bytes is 999
    final int totalSize =
        groups.get(0).stream()
            .map(Record::getData)
            .map(ByteBuffer::array)
            .mapToInt(a -> a.length)
            .sum();
    assertThat(totalSize, is(noOfMessages));

    val totalRecordCount = groups.stream().mapToInt(List::size).sum();
    assertThat(totalRecordCount, is(noOfMessages));
  }
}
