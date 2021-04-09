package com.whylogs.core;

import static org.apache.commons.lang3.ArrayUtils.toObject;

import com.whylogs.core.message.FrequentNumbersSummary;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.Stream;
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
    val builder = StringsSummary.newBuilder().setUniqueCount(uniqueCount);

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

    val items =
        numberTracker.getFrequentNumbers().getFrequentItems(0, ErrorType.NO_FALSE_NEGATIVES);
    val frequentNumbers = FrequentNumbersSummary.newBuilder();

    for (int i = 0; i < items.length; i++) {
      val item = items[i];
      final String content = item.getItem();
      if (content.contains(".")) {
        val value =
            FrequentNumbersSummary.FrequentDoubleItem.newBuilder()
                .setValue(Double.parseDouble(content))
                .setEstimate(item.getEstimate())
                .setRank(i)
                .build();
        frequentNumbers.addDoubles(value);
      } else {
        val value =
            FrequentNumbersSummary.FrequentLongItem.newBuilder()
                .setValue(Long.parseLong(content))
                .setEstimate(item.getEstimate())
                .setRank(i)
                .build();
        frequentNumbers.addLongs(value);
      }
    }
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
    for (int index = 0; index < qvals.length; index++) boxedQvals[index] = (double) (qvals[index]);

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
        .setFrequentNumbers(frequentNumbers)
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

  public static HistogramSummary fromUpdateDoublesSketch(KllFloatsSketch sketch) {
    val n = sketch.getN();
    float start = sketch.getMinValue();
    float end = sketch.getMaxValue();

    val builder = HistogramSummary.newBuilder().setStart(start).setEnd(end);

    // try to be smart here. We don't really have a "histogram"
    // if there are too few data points or there's no band
    if (n < 2 || start == end) {
      val longs = new ArrayList<Long>();
      for (int i = 0; i < n; i++) {
        longs.add(0L);
      }
      return builder.setWidth(0).addAllCounts(longs).build();
    }

    int numberOfBuckets = (int) Math.min(Math.ceil(n / 4.0), 100);
    val width = (end - start) / (numberOfBuckets * 1.0f);
    builder.setWidth(width);

    // calculate histograms from PMF
    val splitPoints = new float[numberOfBuckets];
    for (int i = 0; i < numberOfBuckets; i++) {
      splitPoints[i] = start + i * width;
    }
    val pmf = sketch.getPMF(splitPoints);
    int len = pmf.length - 1;
    for (int i = 0; i < len; i++) {
      builder.addCounts(Math.round(pmf[i] * sketch.getN()));
    }

    return builder.build();
  }
}
