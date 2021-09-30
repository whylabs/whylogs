package com.whylogs.core.utils.sketches;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.lessThan;

import java.util.Random;
import lombok.val;
import org.apache.datasketches.theta.Union;
import org.testng.annotations.Test;

public class ThetaSketchTest {
  @Test
  public void empty_roundtrip_serialization_success() {
    val union = Union.builder().buildUnion();
    val serialized = ThetaSketch.serialize(union);
    ThetaSketch.deserialize(serialized);
  }

  @Test
  public void with_data_roundtrip_serialization_success() {
    val union = Union.builder().buildUnion();
    val rand = new Random();
    for (int i = 0; i < 10000; i++) {
      union.update(Math.abs(rand.nextInt() % 100));
    }

    val serialized = ThetaSketch.serialize(union);
    val roundtrip = ThetaSketch.deserialize(serialized);
    assertThat(roundtrip.getResult().getEstimate() - 100, lessThan(1.0));
  }
}
