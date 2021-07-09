package com.whylogs.core.statistics.datatypes;

import com.google.protobuf.ByteString;
import com.whylogs.core.message.StringsMessage;
import com.whylogs.core.statistics.NumberTracker;
import com.whylogs.core.utils.sketches.ThetaSketch;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;
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
  public static Function<String, List<String>> TOKENIZER = str -> Arrays.asList(str.split(" "));

  public static final ArrayOfStringsSerDe ARRAY_OF_STRINGS_SER_DE = new ArrayOfStringsSerDe();
  // be careful to not use 32 here - somehow the sketches are empty
  public static final int MAX_FREQUENT_ITEM_SIZE = 128;

  private long count;

  // sketches
  private final ItemsSketch<String> items;
  private final Union thetaSketch;
  private final NumberTracker length;
  private final NumberTracker tokenLength;
  private final CharPosTracker charPosTracker;
  @Builder.Default private Function<String, List<String>> tokenizer = TOKENIZER;

  public StringTracker() {
    this.count = 0L;
    this.items = new ItemsSketch<>(MAX_FREQUENT_ITEM_SIZE); // TODO: make this value configurable
    this.thetaSketch = Union.builder().buildUnion();
    this.length = new NumberTracker();
    this.tokenLength = new NumberTracker();
    this.charPosTracker = new CharPosTracker();
    this.tokenizer = TOKENIZER;
  }

  public void update(String value) {
    update(value, null);
  }

  public void update(String value, String charString) {
    if (value == null) {
      return;
    }

    count++;
    thetaSketch.update(value);
    items.update(value);
    charPosTracker.update(value, charString);
    length.track(value.length());
    // TODO allow updates of tokenizer
    tokenLength.track(tokenizer.apply(value).size());
  }

  public void update(String value, String charString, Function<String, List<String>> tokenizer) {
    if (tokenizer != null) {
      this.tokenizer = tokenizer;
    }
    update(value, charString);
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

    val newLength = length.merge(other.length);
    val newTokenLength = tokenLength.merge(other.tokenLength);
    val newCharPostTracker = charPosTracker.merge(other.charPosTracker);

    return StringTracker.builder()
        .count(this.count + other.count)
        .items(itemsCopy)
        .thetaSketch(thetaUnion)
        .length(newLength)
        .tokenLength(newTokenLength)
        .charPosTracker(newCharPostTracker)
        .build();
  }

  public StringsMessage.Builder toProtobuf() {
    return StringsMessage.newBuilder()
        .setCount(count)
        .setItems(ByteString.copyFrom(items.toByteArray(ARRAY_OF_STRINGS_SER_DE)))
        .setCompactTheta(ThetaSketch.serialize(thetaSketch))
        .setLength(length.toProtobuf())
        .setTokenLength(tokenLength.toProtobuf())
        .setCharPosTracker(charPosTracker.toProtobuf());
  }

  public static StringTracker fromProtobuf(StringsMessage message) {
    ItemsSketch<String> items = null;
    val ba = message.getItems().toByteArray();
    if (ba.length > 8) {
      val iMem = Memory.wrap(ba);
      items = ItemsSketch.getInstance(iMem, ARRAY_OF_STRINGS_SER_DE);
    }

    val builder =
        StringTracker.builder()
            .count(message.getCount())
            .items(items)
            .thetaSketch(ThetaSketch.deserialize(message.getCompactTheta()));

    // backward compatibility - only decode these messages if they exist
    if (message.getLength().toByteArray().length > 0) {
      builder
          .length(NumberTracker.fromProtobuf(message.getLength()))
          .tokenLength(NumberTracker.fromProtobuf(message.getTokenLength()))
          .charPosTracker(CharPosTracker.fromProtobuf(message.getCharPosTracker()));
    }
    return builder.build();
  }
}
