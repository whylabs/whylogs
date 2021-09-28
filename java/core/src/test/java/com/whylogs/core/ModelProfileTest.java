package com.whylogs.core;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

import com.whylogs.core.message.ModelProfileMessage;
import com.whylogs.core.metrics.ModelMetrics;
import lombok.val;
import org.testng.annotations.Test;

public class ModelProfileTest {

  @Test
  public void handle_empty_metrics_msg() {
    val withoutMetrics =
        ModelProfileMessage.newBuilder().addOutputFields("foo").addOutputFields("bar").build();
    assertThat(ModelProfile.fromProtobuf(withoutMetrics), nullValue());
  }

  @Test
  public void handle_empty_fields() {
    val mm = new ModelMetrics("pred", "target", "score");
    val withoutMetrics = ModelProfileMessage.newBuilder().setMetrics(mm.toProtobuf()).build();
    assertThat(ModelProfile.fromProtobuf(withoutMetrics), notNullValue());
  }

  @Test
  public void handle_empty_msg() {
    val emptyMessage = ModelProfileMessage.newBuilder().build();
    assertThat(ModelProfile.fromProtobuf(emptyMessage), nullValue());
  }
}
