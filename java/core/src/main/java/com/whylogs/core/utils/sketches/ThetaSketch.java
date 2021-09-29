package com.whylogs.core.utils.sketches;

import com.google.protobuf.ByteString;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.theta.Sketch;
import org.apache.datasketches.theta.Union;

@UtilityClass
public class ThetaSketch {
  public ByteString serialize(Union thetaUnionSketch) {
    return ByteString.copyFrom(thetaUnionSketch.getResult().toByteArray());
  }

  public Union deserialize(ByteString data) {
    val sketch = Sketch.heapify(Memory.wrap(data.toByteArray()));
    val union = Union.builder().buildUnion();
    union.update(sketch);
    return union;
  }
}
