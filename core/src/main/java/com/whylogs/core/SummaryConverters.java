package com.whylogs.core;

import static org.apache.commons.lang3.ArrayUtils.toObject;

import com.whylogs.core.message.FrequentStringsSummary;
import com.whylogs.core.message.HistogramSummary;
import com.whylogs.core.message.NumberSummary;
import com.whylogs.core.message.QuantileSummary;
import com.whylogs.core.message.SchemaSummary;
import com.whylogs.core.message.StringsSummary;
import com.whylogs.core.message.UniqueCountSummary;
import com.whylogs.core.statistics.NumberTracker;
import com.whylogs.core.statistics.SchemaTracker;
import com.whylogs.core.statistics.datatypes.StringTracker;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nullable;
import lombok.val;
import org.apache.datasketches.frequencies.ErrorType;
import org.apache.datasketches.frequencies.ItemsSketch;
import org.apache.datasketches.frequencies.ItemsSketch.Row;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.theta.Union;

public class SummaryConverters {

  public static UniqueCountSummary fromSketch(Union sketch) {
    val result = sketch.getResult();
    return UniqueCountSummary.newBuilder()
        .setEstimate(result.getEstimate())
        .setUpper(result.getUpperBound(1))
        .setLower(result.getLowerBound(1))
        .build();
  }

  public static StringsSummary fromStringTracker(StringTracker tracker) {
    if (tracker == null) {
      return null;
    }
    if (tracker.getCount() == 0) {
      return null;
    }

    val uniqueCount = fromSketch(tracker.getThetaSketch());
    val builder =
        StringsSummary.newBuilder()
            .setUniqueCount(uniqueCount)
            .setLength(fromNumberTracker(tracker.getLength()))
            .setTokenLength(fromNumberTracker(tracker.getTokenLength()))
            .setCharPosTracker(tracker.getCharPosTracker().toSummary());

    // TODO: make this value (100) configurable
    if (uniqueCount.getEstimate() < 100) {
      val frequentStrings = fromStringSketch(tracker.getItems());
      if (frequentStrings != null) {
        builder.setFrequent(frequentStrings);
      }
    }

    return builder.build();
  }

  public static SchemaSummary.Builder fromSchemaTracker(SchemaTracker tracker) {
    val typeCounts = tracker.getTypeCounts();
    val typeCountWithNames =
        typeCounts.entrySet().stream()
            .collect(Collectors.toMap(e -> e.getKey().name(), Entry::getValue));
    return SchemaSummary.newBuilder()
        .setInferredType(tracker.getInferredType())
        .putAllTypeCounts(typeCountWithNames);
  }

  public static NumberSummary fromNumberTracker(NumberTracker numberTracker) {
    if (numberTracker == null) {
      return null;
    }

    long count = numberTracker.getVariance().getCount();

    if (count == 0) {
      return null;
    }

    double stddev = numberTracker.getVariance().stddev();
    double mean, min, max;

    val doubles = numberTracker.getDoubles().toProtobuf();
    if (doubles.getCount() > 0) {
      mean = doubles.getSum() / doubles.getCount();
      min = doubles.getMin();
      max = doubles.getMax();
    } else {
      mean = numberTracker.getLongs().getMean();
      min = (double) numberTracker.getLongs().getMin();
      max = (double) numberTracker.getLongs().getMax();
    }

    val histogram = fromUpdateDoublesSketch(numberTracker.getHistogram());

    val result = numberTracker.getThetaSketch().getResult();
    val uniqueCountSummary =
        UniqueCountSummary.newBuilder()
            .setEstimate(result.getEstimate())
            .setLower(result.getLowerBound(1))
            .setUpper(result.getUpperBound(1));

    // some unfortunate type mismatches requires conversions.
    val QUANTILES = new double[] {0.0, 0.01, 0.05, 0.25, 0.5, 0.75, 0.95, 0.99, 1.0};
    val boxedQuantiles = toObject(QUANTILES);

    // unfortunately ArrayUtils does not have a static utility for converting float[] -> double[].
    val qvals = numberTracker.getHistogram().getQuantiles(QUANTILES);
    val len = qvals.length;
    val boxedQvals = new Double[len];
    for (int index = 0; index < qvals.length; index++) {
      boxedQvals[index] = (double) (qvals[index]);
    }

    val quantileSummary =
        QuantileSummary.newBuilder()
            .addAllQuantiles(Arrays.asList(boxedQuantiles))
            .addAllQuantileValues(Arrays.asList(boxedQvals));

    return NumberSummary.newBuilder()
        .setCount(count)
        .setStddev(stddev)
        .setMin(min)
        .setMax(max)
        .setMean(mean)
        .setHistogram(histogram)
        .setUniqueCount(uniqueCountSummary)
        .setQuantiles(quantileSummary)
        .setIsDiscrete(false) // TODO: migrate Python code over
        .build();
  }

  public static FrequentStringsSummary fromStringSketch(ItemsSketch<String> sketch) {
    val frequentItems = sketch.getFrequentItems(ErrorType.NO_FALSE_NEGATIVES);

    if (frequentItems.length == 0) {
      return null;
    }

    val result =
        Stream.of(frequentItems)
            .map(SummaryConverters::toFrequentItem)
            .collect(Collectors.toList());

    return FrequentStringsSummary.newBuilder().addAllItems(result).build();
  }

  private static FrequentStringsSummary.FrequentItem toFrequentItem(Row<String> row) {
    return FrequentStringsSummary.FrequentItem.newBuilder()
        .setValue(row.getItem())
        .setEstimate(row.getEstimate())
        .build();
  }

  public static HistogramSummary fromUpdateDoublesSketch(final KllFloatsSketch sketch) {
    return fromUpdateDoublesSketch(sketch, 30);
  }

  public static HistogramSummary fromUpdateDoublesSketch(
      final KllFloatsSketch sketch, float[] splitpoints) {
    return fromUpdateDoublesSketch(sketch, 0, splitpoints);
  }

  public static HistogramSummary fromUpdateDoublesSketch(
      final KllFloatsSketch sketch, final int nBins) {
    return fromUpdateDoublesSketch(sketch, nBins, null);
  }

  private static HistogramSummary fromUpdateDoublesSketch(
      final KllFloatsSketch sketch, int nBins, @Nullable float[] splitPoints) {
    nBins = splitPoints != null ? splitPoints.length + 1 : (nBins > 0 ? nBins : 30);
    if (nBins < 2) {
      throw new IllegalArgumentException("at least 2 bins expected");
    }
    val builder = HistogramSummary.newBuilder();
    if (sketch.isEmpty()) {
      return builder.build();
    }
    val n = sketch.getN();
    float start = noNan(sketch.getMinValue());
    float end = noNan(sketch.getMaxValue());
    builder
        .setN(n) //
        .setMin(start) //
        .setMax(end);

    // calculate equally spaced points between [start, end]
    float width = (end - start) / nBins;
    width = Math.max(width, Math.ulp(start)); // min width in case start==end
    builder.setWidth(width);

    if (splitPoints != null) {
      builder.addBins(sketch.getMinValue());
      for (float splitPoint : splitPoints) {
        builder.addBins(splitPoint);
      }
      builder.addBins(sketch.getMaxValue());
    } else {
      // calculate equally spaced points between [start, end]
      // splitPoints must be unique and monotonically increasing
      final int noSplitPoints = nBins - 1;
      splitPoints = new float[noSplitPoints];
      builder.addBins(start);
      for (int i = 0; i < noSplitPoints; i++) {
        splitPoints[i] = start + (i + 1) * width;
        builder.addBins(splitPoints[i]);
      }
      builder.addBins(end);
    }

    for (double v : sketch.getPMF(splitPoints)) {
      // scale fractions to counts
      builder.addCounts(Math.round(noNan(v) * n));
    }
    return builder.build();
  }

  // noNan for Double streams
  public static double noNan(double value) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      return 0f;
    }
    return value;
  }

  // noNan for Float streams
  public static float noNan(float value) {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      return 0f;
    }
    return value;
  }
}
