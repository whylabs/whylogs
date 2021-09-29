package com.whylogs.core.utils.sketches;

import com.google.protobuf.ByteString;
import com.whylogs.core.message.FrequentItemsSketchMessage;
import com.whylogs.core.message.FrequentNumbersSketchMessage;
import lombok.experimental.UtilityClass;
import lombok.val;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;

@UtilityClass
public class FrequentStringsSketch {
  public static final int FREQUENT_MAX_LG_K = 7;

  public static final ArrayOfStringsSerDe ARRAY_OF_STRINGS_SER_DE = new ArrayOfStringsSerDe();

  public ItemsSketch<String> create() {
    return new ItemsSketch<>((int) Math.pow(2, FREQUENT_MAX_LG_K));
  }

  public ByteString serialize(ItemsSketch<String> sketch) {
    return ByteString.copyFrom(sketch.toByteArray(ARRAY_OF_STRINGS_SER_DE));
  }

  public FrequentItemsSketchMessage.Builder toStringSketch(ItemsSketch<String> sketch) {
    return FrequentItemsSketchMessage.newBuilder()
        .setLgMaxK(FREQUENT_MAX_LG_K)
        .setSketch(ByteString.copyFrom(sketch.toByteArray(ARRAY_OF_STRINGS_SER_DE)));
  }

  public FrequentNumbersSketchMessage.Builder toNumbersMessage(ItemsSketch<String> sketch) {
    return FrequentNumbersSketchMessage.newBuilder()
        .setLgMaxK(FREQUENT_MAX_LG_K)
        .setSketch(ByteString.copyFrom(sketch.toByteArray(ARRAY_OF_STRINGS_SER_DE)));
  }

  public ItemsSketch<String> deserialize(ByteString bytes) {
    val data = bytes.toByteArray();

    ItemsSketch<String> freqNumbers;
    if (data.length > 8) {
      return ItemsSketch.getInstance(Memory.wrap(data), ARRAY_OF_STRINGS_SER_DE);
    } else {
      // Create an empty sketch
      return create();
    }
  }
}
