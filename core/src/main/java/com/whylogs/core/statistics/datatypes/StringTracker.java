package com.whylogs.core.statistics.datatypes;

import com.google.protobuf.ByteString;
import com.whylogs.core.message.StringsMessage;
import com.whylogs.core.utils.sketches.ThetaSketch;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.val;
import org.apache.datasketches.ArrayOfStringsSerDe;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.memory.Memory;
import org.apache.datasketches.memory.WritableMemory;
import org.apache.datasketches.theta.Union;

@Builder
@Getter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringTracker {
  public static final ArrayOfStringsSerDe ARRAY_OF_STRINGS_SER_DE = new ArrayOfStringsSerDe();
  // be careful to not use 32 here - somehow the sketches are empty
  public static final int MAX_FREQUENT_ITEM_SIZE = 128;

  private long count;

  // sketches
  private final ItemsSketch<String> items;
  private final Union thetaSketch;

  public StringTracker() {
    this.count = 0L;
    this.items = new ItemsSketch<>(MAX_FREQUENT_ITEM_SIZE); // TODO: make this value configurable
    this.thetaSketch = Union.builder().buildUnion();
  }

  public void update(String value) {
    if (value == null) {
      return;
    }

    count++;
    thetaSketch.update(value);
    items.update(value);
  }

  /**
   * Merge this StringTracker object with another. This merges the sketches as well
   *
   * @param other the other String tracker to merge
   * @return a new StringTracker object
   */
  public StringTracker merge(StringTracker other) {
    val bytes = this.items.toByteArray(ARRAY_OF_STRINGS_SER_DE);
    val itemsCopy = ItemsSketch.getInstance(WritableMemory.wrap(bytes), ARRAY_OF_STRINGS_SER_DE);
    itemsCopy.merge(other.items);

    val thetaUnion = Union.builder().buildUnion();
    thetaUnion.update(this.thetaSketch.getResult());
    thetaUnion.update(other.thetaSketch.getResult());

    return new StringTracker(this.count + other.count, itemsCopy, thetaUnion);
  }

  public StringsMessage.Builder toProtobuf() {
    return StringsMessage.newBuilder()
        .setCount(count)
        .setItems(ByteString.copyFrom(items.toByteArray(ARRAY_OF_STRINGS_SER_DE)))
        .setCompactTheta(ThetaSketch.serialize(thetaSketch));
  }

  public static StringTracker fromProtobuf(StringsMessage message) {
    val iMem = Memory.wrap(message.getItems().toByteArray());
    val items = ItemsSketch.getInstance(iMem, ARRAY_OF_STRINGS_SER_DE);

    return StringTracker.builder()
        .count(message.getCount())
        .items(items)
        .thetaSketch(ThetaSketch.deserialize(message.getCompactTheta()))
        .build();
  }
}
