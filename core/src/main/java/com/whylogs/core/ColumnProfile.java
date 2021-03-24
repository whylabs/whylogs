package com.whylogs.core;

import static com.whylogs.core.SummaryConverters.fromSchemaTracker;
import static com.whylogs.core.statistics.datatypes.StringTracker.ARRAY_OF_STRINGS_SER_DE;
import static com.whylogs.core.types.TypedDataConverter.NUMERIC_TYPES;

import com.google.common.base.Preconditions;
import com.google.protobuf.ByteString;
import com.whylogs.core.message.ColumnMessage;
import com.whylogs.core.message.ColumnSummary;
import com.whylogs.core.message.HllSketchMessage;
import com.whylogs.core.statistics.CountersTracker;
import com.whylogs.core.statistics.NumberTracker;
import com.whylogs.core.statistics.SchemaTracker;
import com.whylogs.core.types.TypedDataConverter;
import com.whylogs.core.utils.sketches.FrequentStringsSketch;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NonNull;
import lombok.val;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.hll.HllSketch;
import org.apache.datasketches.hll.Union;
import org.apache.datasketches.memory.Memory;

@AllArgsConstructor(access = AccessLevel.PRIVATE)
@Getter
@Builder(setterPrefix = "set")
public class ColumnProfile {
  public static final int FREQUENT_MAX_LG_K = 7;
  private static final int CARDINALITY_LG_K = 12;

  @NonNull private final String columnName;
  @NonNull private final CountersTracker counters;
  @NonNull private final SchemaTracker schemaTracker;
  @NonNull private final NumberTracker numberTracker;
  @NonNull private final ItemsSketch<String> frequentItems;
  @NonNull private final HllSketch cardinalityTracker;

  public ColumnProfile(String columnName) {
    this.columnName = columnName;
    this.counters = new CountersTracker();
    this.schemaTracker = new SchemaTracker();
    this.numberTracker = new NumberTracker();
    this.frequentItems = FrequentStringsSketch.create();
    this.cardinalityTracker = new HllSketch(CARDINALITY_LG_K);
  }

  public void track(Object value) {
    synchronized (this) {
      counters.incrementCount();

      if (value == null) {
        counters.incrementNull();
        return;
      }

      // TODO: ignore this if we already know the data type
      val typedData = TypedDataConverter.convert(value);
      schemaTracker.track(typedData.getType());

      switch (typedData.getType()) {
        case FRACTIONAL:
          final double fractional = typedData.getFractional();
          trackText(String.valueOf(fractional));
          if (Double.isNaN(fractional) || Double.isInfinite(fractional)) {
            counters.incrementNull();
          } else {
            numberTracker.track(fractional);
          }
          break;
        case INTEGRAL:
          final long integralValue = typedData.getIntegralValue();
          trackText(String.valueOf(integralValue));
          numberTracker.track(integralValue);
          break;
        case BOOLEAN:
          // TODO: handle boolean across languages? Python booleans are "True" vs Java "true"
          trackText(String.valueOf(typedData.isBooleanValue()));
          if (typedData.isBooleanValue()) {
            counters.incrementTrue();
          }
          break;
        case STRING:
          trackText(typedData.getStringValue());
      }
    }
  }

  private void trackText(String text) {
    frequentItems.update(text);
    cardinalityTracker.update(text);
  }

  public ColumnSummary toColumnSummary() {
    val schema = fromSchemaTracker(schemaTracker);

    val builder = ColumnSummary.newBuilder().setCounters(counters.toProtobuf());
    if (schema != null) {
      builder.setSchema(schema);
      if (NUMERIC_TYPES.contains(schema.getInferredType().getType())) {
        val numberSummary = SummaryConverters.fromNumberTracker(this.numberTracker);
        if (numberSummary != null) {
          builder.setNumberSummary(numberSummary);
        }
      }
    }

    return builder.build();
  }

  public ColumnProfile merge(ColumnProfile other) {
    return this.merge(other, true);
  }

  public ColumnProfile merge(ColumnProfile other, boolean checkName) {
    if (checkName) {
      Preconditions.checkArgument(
          this.columnName.equals(other.columnName),
          String.format(
              "Mismatched column name. Expected [%s], got [%s]",
              this.columnName, other.columnName));
    }

    val mergedSketch = Union.heapify(this.cardinalityTracker.toCompactByteArray());
    mergedSketch.update(other.cardinalityTracker);

    val iMem = Memory.wrap(frequentItems.toByteArray(ARRAY_OF_STRINGS_SER_DE));
    val copyFreqItems = ItemsSketch.getInstance(iMem, ARRAY_OF_STRINGS_SER_DE);
    copyFreqItems.merge(other.frequentItems);

    return ColumnProfile.builder()
        .setColumnName(this.columnName)
        .setCounters(this.counters.merge(other.counters))
        .setNumberTracker(this.numberTracker.merge(other.numberTracker))
        .setSchemaTracker(this.schemaTracker.merge(other.schemaTracker))
        .setCardinalityTracker(HllSketch.heapify(mergedSketch.toCompactByteArray()))
        .setFrequentItems(copyFreqItems)
        .build();
  }

  public ColumnMessage.Builder toProtobuf() {
    val hllSketchMessage =
        HllSketchMessage.newBuilder()
            .setLgK(cardinalityTracker.getLgConfigK())
            .setSketch(ByteString.copyFrom(cardinalityTracker.toCompactByteArray()));

    return ColumnMessage.newBuilder()
        .setName(columnName)
        .setCounters(counters.toProtobuf())
        .setSchema(schemaTracker.toProtobuf())
        .setNumbers(numberTracker.toProtobuf())
        .setCardinalityTracker(hllSketchMessage)
        .setFrequentItems(FrequentStringsSketch.toStringSketch(this.frequentItems));
  }

  public static ColumnProfile fromProtobuf(ColumnMessage message) {
    return ColumnProfile.builder()
        .setColumnName(message.getName())
        .setCounters(CountersTracker.fromProtobuf(message.getCounters()))
        .setSchemaTracker(SchemaTracker.fromProtobuf(message.getSchema()))
        .setNumberTracker(NumberTracker.fromProtobuf(message.getNumbers()))
        .setCardinalityTracker(
            HllSketch.heapify(message.getCardinalityTracker().getSketch().toByteArray()))
        .setFrequentItems(FrequentStringsSketch.deserialize(message.getFrequentItems().getSketch()))
        .build();
  }
}
