package com.whylabs.logging.firehose;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehose;
import com.amazonaws.services.kinesisfirehose.AmazonKinesisFirehoseClient;
import com.amazonaws.services.kinesisfirehose.model.PutRecordBatchRequest;
import com.whylabs.logging.core.DatasetProfile;
import com.whylabs.logging.firehose.iterator.FirehoseRecordGrouper;
import lombok.val;

public class FirehosePublisher {
  private final AmazonKinesisFirehose firehose;
  private final String deliverStream;

  public FirehosePublisher(String region, String deliverStream) {
    this.firehose =
        AmazonKinesisFirehoseClient.builder()
            .withRegion(region)
            .withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
            .build();
    this.deliverStream = deliverStream;
  }

  public void putProfile(final DatasetProfile profile) {
    val it = profile.toChunkIterator();
    val groupedRecords = new FirehoseRecordGrouper(it);
    while (groupedRecords.hasNext()) {
      val batchPut =
          new PutRecordBatchRequest()
              .withDeliveryStreamName(deliverStream)
              .withRecords(groupedRecords.next());

      firehose.putRecordBatch(batchPut);
    }
  }
}
