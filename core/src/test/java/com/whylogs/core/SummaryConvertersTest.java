package com.whylogs.core;

import static org.testng.Assert.assertEquals;

import com.google.common.collect.ImmutableMap;
import com.google.protobuf.ListValue;
import com.google.protobuf.Value;
import com.google.protobuf.util.JsonFormat;
import com.whylogs.core.message.ColumnSummary;
import com.whylogs.core.message.ConfusionMatrix;
import com.whylogs.core.message.DatasetSummary;
import com.whylogs.core.message.MetricsSummary;
import com.whylogs.core.message.ModelSummary;
import com.whylogs.core.message.ModelType;
import com.whylogs.core.message.ROCCurve;
import com.whylogs.core.message.RecallCurve;
import com.whylogs.core.statistics.NumberTracker;
import java.util.Arrays;
import lombok.SneakyThrows;
import lombok.val;
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

  private static String expectedJson =
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
}
