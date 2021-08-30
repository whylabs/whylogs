package com.whylogs.core;

import static com.whylogs.core.SummaryConverters.fromUpdateDoublesSketch;
import static org.apache.commons.lang3.ArrayUtils.removeAll;
import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.common.primitives.Floats;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.whylogs.core.message.ColumnSummary;
import com.whylogs.core.message.ConfusionMatrix;
import com.whylogs.core.message.DatasetSummary;
import com.whylogs.core.message.HistogramSummary;
import com.whylogs.core.message.MetricsSummary;
import com.whylogs.core.message.ModelSummary;
import com.whylogs.core.message.ModelType;
import com.whylogs.core.message.ROCCurve;
import com.whylogs.core.message.RecallCurve;
import com.whylogs.core.statistics.NumberTracker;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.val;
import org.apache.datasketches.kll.KllFloatsSketch;
import org.apache.datasketches.memory.Memory;
import org.testng.annotations.Test;

public class SummaryConvertersTest {
  @Test
  public void convert_numbertracker_to_summary() {
    val numberTracker = new NumberTracker();
    final int count = 1000;
    for (int i = 0; i < count; i++) {
      numberTracker.track(i);
    }
    val summary = SummaryConverters.fromNumberTracker(numberTracker);
    assertEquals(summary.getCount(), count);
  }

  private static ListValue.Builder toListValue(Double[] dlist) {
    val lvBuilder = ListValue.newBuilder();
    for (val d : dlist) {
      lvBuilder.addValues(Value.newBuilder().setNumberValue(d));
    }
    return lvBuilder;
  }

  private static ListValue.Builder toListValue(Long[] dlist) {
    val lvBuilder = ListValue.newBuilder();
    for (val d : dlist) {
      lvBuilder.addValues(Value.newBuilder().setNumberValue(d));
    }
    return lvBuilder;
  }

  private static final String expectedJson =
      "{\"columns\":{\"feature1\":{}},\"model\":{\"metrics\":{\"model_type\":\"CLASSIFICATION\",\"roc_fpr_tpr\":{\"values\":[[1.0,1.0],[1.0,1.0],[1.0,1.0]]},\"recall_prec\":{\"values\":[[1.0,0.42857142857142855],[1.0,0.42857142857142855],[1.0,0.42857142857142855]]},\"confusion_matrix\":{\"labels\":[\"0\",\"1\"],\"target_field\":\"targets\",\"predictions_field\":\"targets\",\"score_field\":\"scores\",\"counts\":[[33.0,6.0],[11.0,27.0]]}}}}";

  /*
     Verify that JSON encoded dataset summary matches expectations and is the same as json produced by python client.
     Also serves as reference example of how to build a MetricsSummary protobuf message in Java.
  */
  @SneakyThrows
  @Test
  public void dataset_metrics_to_json() {
    val ROC =
        Arrays.asList(new Double[] {1.0, 1.0}, new Double[] {1.0, 1.0}, new Double[] {1.0, 1.0});
    val rocCurve = ROCCurve.newBuilder();
    for (val obj : ROC) {
      rocCurve.addValues(toListValue(obj));
    }

    val precision =
        Arrays.asList(
            new Double[] {1.0, 0.42857142857142855},
            new Double[] {1.0, 0.42857142857142855},
            new Double[] {1.0, 0.42857142857142855});
    val recallPrecision = RecallCurve.newBuilder();
    for (val obj : precision) {
      recallPrecision.addValues(toListValue(obj));
    }

    val cmBuilder =
        ConfusionMatrix.newBuilder()
            .addAllLabels(Arrays.asList("0", "1"))
            .setTargetField("targets")
            .setPredictionsField("targets")
            .setScoreField("scores");
    val confusion = Arrays.asList(new Long[] {33L, 6L}, new Long[] {11L, 27L});
    for (val obj : confusion) {
      cmBuilder.addCounts(toListValue(obj));
    }

    val msbuilder =
        MetricsSummary.newBuilder()
            .setModelType(ModelType.CLASSIFICATION)
            .setRocFprTpr(rocCurve)
            .setRecallPrec(recallPrecision)
            .setConfusionMatrix(cmBuilder);

    val summaryColumns = ImmutableMap.of("feature1", ColumnSummary.newBuilder().build());
    val modelSummary = ModelSummary.newBuilder().setMetrics(msbuilder);
    val summary = DatasetSummary.newBuilder().putAllColumns(summaryColumns).setModel(modelSummary);

    val str =
        JsonFormat.printer()
            .preservingProtoFieldNames()
            .sortingMapKeys()
            .omittingInsignificantWhitespace()
            .print(summary);
    assertEquals(str, expectedJson);
  }

  /** test construction of histograms specifying number of bins */
  @Test
  public void summary_histogram_nbins() {
    val nbins = 10;
    val sketch = new KllFloatsSketch(256);

    HistogramSummary summary = fromUpdateDoublesSketch(sketch, nbins);
    // an empty sketch should have no histogram
    assertEquals(0, summary.getCountsList().size());

    sketch.update(10L);
    summary = fromUpdateDoublesSketch(sketch, nbins);
    // all non-empty histograms should have `nbin` counts
    assertEquals(nbins, summary.getCountsList().size());
    // minValue is in left-most bin...
    assertEquals(1.0D, summary.getCounts(0), 0.0);
    // and all other bins are empty
    for (int i = 1; i < summary.getCountsList().size(); i++) {
      assertEquals(0.0D, summary.getCounts(i), 0.0);
    }

    sketch.update(11L);
    summary = fromUpdateDoublesSketch(sketch, nbins);
    assertEquals(nbins, summary.getCountsList().size());
    // minValue is in left-most bin...
    assertEquals(1.0D, summary.getCounts(0), 0.0);
    // maxValue is in right-most bin...
    assertEquals(1.0D, summary.getCounts(nbins - 1), 0.0);
    // and all other bins are empty
    for (int i = 1; i < summary.getCountsList().size() - 1; i++) {
      assertEquals(0.0D, summary.getCounts(i), 0.0);
    }

    sketch.update(12L);
    summary = fromUpdateDoublesSketch(sketch, nbins);
    assertEquals(nbins, summary.getCountsList().size());
    assertEquals(1.0D, summary.getCounts(0), 0.0);
    assertEquals(1.0D, summary.getCounts(nbins - 1), 0.0);
    assertEquals(1.0D, summary.getCounts(nbins / 2), 0.0);
  }

  // reproducible pseudo-random generator.
  private static final java.util.Random rand = new java.util.Random(0xcafebabe);

  private static float randFloat(int min, int max) {
    return (float) rand.nextInt((max - min) + 1) + min;
  }

  // There is no copy or clone method on KllFloatsSketch!!
  private static KllFloatsSketch clone(KllFloatsSketch sketch) {
    return KllFloatsSketch.heapify(Memory.wrap(sketch.toByteArray()));
  }

  /**
   * test construction of histograms specifying split points. This functionality will typically be
   * used by calculating the split points from an aggregated sketch, then using those points to
   * construct histograms for each of the original sketches.
   */
  @Test
  public void summary_histogram_splitpoints() {
    val nbins = 30;
    val sketch_a = new KllFloatsSketch(256);
    val sketch_b = new KllFloatsSketch(256);

    // track values between 1 and 100.
    for (int i = 0; i < 100; i++) {
      sketch_a.update(randFloat(1, 100));
    }

    // track values between 50 and 300.
    for (int i = 0; i < 500; i++) {
      sketch_b.update(randFloat(50, 300));
    }

    // merge the sketches
    val merged = clone(sketch_a);
    merged.merge(sketch_b);

    // calculate histogram summary
    val summary = fromUpdateDoublesSketch(merged, nbins);

    // use split points to calculate individual histograms
    // strip min and max values from the binlist
    float[] splitpoints = Floats.toArray(summary.getBinsList());
    splitpoints = removeAll(splitpoints, 0, splitpoints.length - 1);
    assertEquals(splitpoints.length, 29);

    val summary_a = fromUpdateDoublesSketch(sketch_a, splitpoints);
    assertEquals(nbins, summary_a.getCountsList().size());
    val summary_b = fromUpdateDoublesSketch(sketch_b, splitpoints);
    assertEquals(nbins, summary_b.getCountsList().size());
  }
}
