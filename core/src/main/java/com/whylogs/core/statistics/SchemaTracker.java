package com.whylogs.core.statistics;

import com.google.common.collect.Maps;
import com.whylogs.core.message.InferredType;
import com.whylogs.core.message.SchemaMessage;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.val;

@EqualsAndHashCode
@RequiredArgsConstructor
public class SchemaTracker {
  private static final InferredType.Builder UNKNOWN_TYPE_BUILDER =
      InferredType.newBuilder().setType(InferredType.Type.UNKNOWN);

  private final Map<InferredType.Type, Long> typeCounts;

  public SchemaTracker() {
    this.typeCounts = new HashMap<>(InferredType.Type.values().length, 1.0f);
  }

  public void track(InferredType.Type type) {
    typeCounts.merge(type, 1L, Long::sum);
  }

  long getCount(InferredType.Type type) {
    return typeCounts.getOrDefault(type, 0L);
  }

  public Map<InferredType.Type, Long> getTypeCounts() {
    return Collections.unmodifiableMap(typeCounts);
  }

  public InferredType getInferredType() {
    val totalCount = typeCounts.values().stream().mapToLong(Long::longValue).sum();
    if (totalCount == 0) {
      return UNKNOWN_TYPE_BUILDER.build();
    }

    // first figure out the most popular type and its count
    val candidate = getMostPopularType(totalCount);
    if (candidate.getRatio() > 0.7) {
      return candidate.build();
    }

    // integral is a subset of fractional
    val fractionalCount =
        Stream.of(InferredType.Type.INTEGRAL, InferredType.Type.FRACTIONAL)
            .mapToLong(type -> typeCounts.getOrDefault(type, 0L))
            .sum();

    // Handling String case first
    // it has to have more entries than fractional values
    val candidateType = candidate.getType();
    if (candidateType == InferredType.Type.STRING
        && typeCounts.get(InferredType.Type.STRING) > fractionalCount) {
      // treat everything else as "String" except UNKNOWN
      val coercedCount =
          Stream.of(
                  InferredType.Type.STRING,
                  InferredType.Type.INTEGRAL,
                  InferredType.Type.FRACTIONAL,
                  InferredType.Type.BOOLEAN)
              .mapToLong(type -> typeCounts.getOrDefault(type, 0L))
              .sum();
      val actualRatio = coercedCount / (double) totalCount;

      return InferredType.newBuilder()
          .setType(InferredType.Type.STRING)
          .setRatio(actualRatio)
          .build();
    }

    // if not string but another type with majority
    if (candidate.getRatio() > 0.5) {
      long actualCount = typeCounts.get(candidateType);
      if (candidateType == InferredType.Type.FRACTIONAL) {
        actualCount = fractionalCount;
      }

      return InferredType.newBuilder()
          .setType(candidateType)
          .setRatio(actualCount / (double) totalCount)
          .build();
    }

    // Otherwise, if fractional count is the majority, then likely this is a fractional type
    final double fractionalRatio = fractionalCount / (double) totalCount;
    if (fractionalRatio > 0.5) {
      return InferredType.newBuilder()
          .setType(InferredType.Type.FRACTIONAL)
          .setRatio(fractionalRatio)
          .build();
    }

    // we can't infer any type
    return InferredType.newBuilder().setType(InferredType.Type.UNKNOWN).setRatio(1.0).build();
  }

  public SchemaMessage.Builder toProtobuf() {
    val protobufFriendlyMap =
        typeCounts.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().getNumber(), Entry::getValue));

    return SchemaMessage.newBuilder().putAllTypeCounts(protobufFriendlyMap);
  }

  public static SchemaTracker fromProtobuf(SchemaMessage message) {
    val schemaTracker = new SchemaTracker();
    message
        .getTypeCountsMap()
        .forEach((k, v) -> schemaTracker.typeCounts.put(InferredType.Type.forNumber(k), v));
    return schemaTracker;
  }

  private InferredType.Builder getMostPopularType(long totalCount) {
    val mostPopularType =
        typeCounts.entrySet().stream()
            .max((e1, e2) -> (int) (e1.getValue() - e2.getValue()))
            .map(Entry::getKey)
            .orElse(InferredType.Type.UNKNOWN);

    val count = typeCounts.getOrDefault(mostPopularType, 0L);
    val ratio = count * 1.0 / totalCount;

    return InferredType.newBuilder().setType(mostPopularType).setRatio(ratio);
  }

  public void add(SchemaTracker other) {
    final InferredType.Type[] allTypes = InferredType.Type.values();
    for (val type : allTypes) {
      if (this.typeCounts.containsKey(type) || other.typeCounts.containsKey(type)) {
        this.typeCounts.merge(type, other.getCount(type), Long::sum);
      }
    }
  }

  public SchemaTracker merge(SchemaTracker other) {
    val thisCopy = new SchemaTracker(Maps.newHashMap(typeCounts));

    final InferredType.Type[] allTypes = InferredType.Type.values();
    for (val type : allTypes) {
      if (this.typeCounts.containsKey(type) || other.typeCounts.containsKey(type)) {
        thisCopy.typeCounts.merge(type, other.getCount(type), Long::sum);
      }
    }

    return thisCopy;
  }
}
