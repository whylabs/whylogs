package com.whylogs.core;

import static com.whylogs.core.SummaryConverters.fromSchemaTracker;
import static com.whylogs.core.statistics.datatypes.StringTracker.ARRAY_OF_STRINGS_SER_DE;
import static com.whylogs.core.types.TypedDataConverter.NUMERIC_TYPES;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.protobuf.ByteString;
import com.whylogs.core.message.ColumnMessage;
import com.whylogs.core.message.ColumnSummary;
import com.whylogs.core.message.HllSketchMessage;
import com.whylogs.core.message.InferredType;
import com.whylogs.core.statistics.CountersTracker;
import com.whylogs.core.statistics.NumberTracker;
import com.whylogs.core.statistics.SchemaTracker;
import com.whylogs.core.statistics.datatypes.StringTracker;
import com.whylogs.core.types.TypedData;
import com.whylogs.core.types.TypedDataConverter;
import com.whylogs.core.utils.sketches.FrequentStringsSketch;
import javax.annotation.Nullable;
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
  public static final int STRING_LENGTH_MAX = 256;
  private static volatile ImmutableSet<String> NULL_STR_ENVS;

  @NonNull private final String columnName;
  @NonNull private final CountersTracker counters;
  @NonNull private final SchemaTracker schemaTracker;
  @NonNull private final NumberTracker numberTracker;
  @NonNull private final ItemsSketch<String> frequentItems;
  @NonNull private final HllSketch cardinalityTracker;
  @NonNull private final ImmutableSet<String> nullStrs;
  @NonNull private final StringTracker stringTracker;

  static ImmutableSet<String> nullStrsFromEnv() {
    if (ColumnProfile.NULL_STR_ENVS == null) {
      val nullSpec = System.getenv("NULL_STRINGS");
      ColumnProfile.NULL_STR_ENVS =
          nullSpec == null ? ImmutableSet.of() : ImmutableSet.copyOf(nullSpec.split(","));
    }
    return ColumnProfile.NULL_STR_ENVS;
  }

  public ColumnProfile(String columnName) {
    this(columnName, nullStrsFromEnv());
  }

  public ColumnProfile(String columnName, ImmutableSet<String> nullStrs) {
    this.columnName = columnName;
    this.counters = new CountersTracker();
    this.schemaTracker = new SchemaTracker();
    this.stringTracker = new StringTracker();
    this.numberTracker = new NumberTracker();
    this.frequentItems = FrequentStringsSketch.create();
    this.cardinalityTracker = new HllSketch(CARDINALITY_LG_K);
    this.nullStrs = nullStrs;
  }

  public void track(Object value) {
    synchronized (this) {
      counters.incrementCount();

      // TODO: ignore this if we already know the data type
      val typedData = TypedDataConverter.convert(value);
      if (isNull(typedData)) {
        schemaTracker.track(InferredType.Type.NULL);
        return;
      }
      schemaTracker.track(typedData.getType());

      switch (typedData.getType()) {
        case FRACTIONAL:
          final double fractional = typedData.getFractional();
          trackText(String.valueOf(fractional));
          numberTracker.track(fractional);
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
          final String stringValue = typedData.getStringValue();

          // trackText is the older tracking code.  It limits the length of strings in order to
          // track cardinality.
          trackText(stringValue);

          // string tracking does not limit the length of input text.
          stringTracker.update(stringValue);
      }
    }
  }

  private boolean isNull(TypedData value) {
    if (value == null) return true;
    if (value.getType() == InferredType.Type.STRING && !this.nullStrs.isEmpty()) {
      return this.nullStrs.contains(value.getStringValue());
    }

    if (value.getType() == InferredType.Type.FRACTIONAL) {
      return Double.isNaN(value.getFractional()) || Double.isInfinite(value.getFractional());
    }

    return false;
  }

  private void trackText(String text) {
    if (text != null && text.length() > STRING_LENGTH_MAX) {
      text = text.substring(0, STRING_LENGTH_MAX);
    }
    frequentItems.update(text);
    cardinalityTracker.update(text);
  }

  public ColumnSummary toColumnSummary() {
    val schema = fromSchemaTracker(schemaTracker);

    val builder = ColumnSummary.newBuilder().setCounters(counters.toProtobuf());
    if (schema != null) {
      builder.setSchema(schema);
      if (NUMERIC_TYPES.contains(schema.getInferredType().getType())) {
        val stringsSummary = SummaryConverters.fromStringTracker(this.stringTracker);

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

    val builder =
        ColumnProfile.builder()
            .setColumnName(this.columnName)
            .setCounters(this.counters.merge(other.counters))
            .setNumberTracker(this.numberTracker.merge(other.numberTracker))
            .setSchemaTracker(this.schemaTracker.merge(other.schemaTracker))
            .setCardinalityTracker(HllSketch.heapify(mergedSketch.toCompactByteArray()))
            .setFrequentItems(copyFreqItems)
            .setNullStrs(Sets.union(this.getNullStrs(), other.getNullStrs()).immutableCopy());

    // backward compatibility with profiles that don't have stringtracker.
    if (this.stringTracker == null) {
      builder.setStringTracker(other.stringTracker);
    } else {
      builder.setStringTracker(this.stringTracker.merge(other.stringTracker));
    }

    return builder.build();
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
        .setStrings(stringTracker.toProtobuf())
        .setCardinalityTracker(hllSketchMessage)
        .setFrequentItems(FrequentStringsSketch.toStringSketch(this.frequentItems));
  }

  @Nullable
  public static ColumnProfile fromProtobuf(@Nullable ColumnMessage message) {
    if (message == null || message.getSerializedSize() == 0) {
      return null;
    }

    val builder =
        ColumnProfile.builder()
            .setColumnName(message.getName())
            .setCounters(CountersTracker.fromProtobuf(message.getCounters()))
            .setSchemaTracker(
                SchemaTracker.fromProtobuf(
                    message.getSchema(), message.getCounters().getNullCount().getValue()))
            .setNumberTracker(NumberTracker.fromProtobuf(message.getNumbers()))
            .setCardinalityTracker(
                HllSketch.heapify(message.getCardinalityTracker().getSketch().toByteArray()))
            .setFrequentItems(
                FrequentStringsSketch.deserialize(message.getFrequentItems().getSketch()))
            .setNullStrs(ColumnProfile.nullStrsFromEnv());

    // backward compatibility - only decode StringsMessage if it exists.
    // older profiles written by java library may not have any StringsMessage.
    StringTracker strTracker;
    if (message.hasStrings()) {
      strTracker = StringTracker.fromProtobuf(message.getStrings());
    } else {
      strTracker = new StringTracker();
    }
    builder.setStringTracker(strTracker);

    return builder.build();
  }
}
