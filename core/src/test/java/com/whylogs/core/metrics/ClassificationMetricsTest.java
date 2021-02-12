package com.whylogs.core.metrics;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Streams;
import com.whylogs.core.DatasetProfile;
import java.time.Instant;
import lombok.val;
import org.apache.commons.lang3.tuple.Pair;
import org.testng.annotations.Test;

@SuppressWarnings("UnstableApiUsage")
public class ClassificationMetricsTest {
  @Test
  public void binaryClassification_should_be_correct() {
    val profile = new DatasetProfile("session", Instant.now());
    val metrics = ClassificationMetrics.of();
    val predictions = ImmutableList.of(0, 1, 1, 0, 0, 1, 1);
    val targets = ImmutableList.of(1, 0, 1, 1, 0, 1, 1);
    Streams.zip(predictions.stream(), targets.stream(), Pair::of)
        .forEach(pair -> metrics.update(profile, pair.getLeft(), pair.getRight(), 0));
    val matrix = metrics.getConfusionMatrix();
    assertThat(matrix.length, is(2));
    assertThat(matrix[0].length, is(2));
    assertThat(matrix[1].length, is(2));

    // Result matrix
    // [1, 2]
    // [1, 3]
    assertThat(matrix[0][0], is(1L));
    assertThat(matrix[0][1], is(2L));
    assertThat(matrix[1][0], is(1L));
    assertThat(matrix[1][1], is(3L));
  }

  @Test
  public void binaryClassification_withBoolean_shouldTransformTo_0s_and_1s() {
    val profile = new DatasetProfile("session", Instant.now());
    val metrics = ClassificationMetrics.of();
    val predictions = ImmutableList.of(false, true, true, false, false, true, true);
    val targets = ImmutableList.of(true, false, true, true, false, true, true);
    Streams.zip(predictions.stream(), targets.stream(), Pair::of)
        .forEach(pair -> metrics.update(profile, pair.getLeft(), pair.getRight(), 0));
    val matrix = metrics.getConfusionMatrix();
    assertThat(matrix.length, is(2));
    assertThat(matrix[0].length, is(2));
    assertThat(matrix[1].length, is(2));

    // Result matrix same as above
    // [1, 2]
    // [1, 3]
    assertThat(matrix[0][0], is(1L));
    assertThat(matrix[0][1], is(2L));
    assertThat(matrix[1][0], is(1L));
    assertThat(matrix[1][1], is(3L));

    val columns = profile.getColumns();
    val predictionColumn = columns.get("whylogs.metrics.predictions");
    assertThat(predictionColumn, is(notNullValue()));
    // verify that we are tracking "0" and "1" for boolean prediction
    assertThat(predictionColumn.getFrequentItems().getEstimate("1"), is(greaterThan(0L)));
    assertThat(predictionColumn.getFrequentItems().getEstimate("0"), is(greaterThan(0L)));
  }


  @Test
  public void binaryClassification_merge_itself() {
    val profile = new DatasetProfile("session", Instant.now());

    val binaryMatrix = ClassificationMetrics.of();
    val predictions = ImmutableList.of(0, 1, 1, 0, 0, 1, 1);
    val targets = ImmutableList.of(1, 0, 1, 1, 0, 1, 1);
    Streams.zip(predictions.stream(), targets.stream(), Pair::of)
        .forEach(pair -> binaryMatrix.update(profile, pair.getLeft(), pair.getRight(), 0));

    // created a merged confusion matrix by merging the original with itself
    val merged = binaryMatrix.merge(binaryMatrix);

    // now run the same data through the existing matrix
    Streams.zip(predictions.stream(), targets.stream(), Pair::of)
        .forEach(pair -> binaryMatrix.update(profile, pair.getLeft(), pair.getRight(), 0));
    val matrix = binaryMatrix.getConfusionMatrix();
    val mergedResult = merged.getConfusionMatrix();
    assertThat(matrix.length, is(2));
    assertThat(matrix[0].length, is(2));
    assertThat(matrix[1].length, is(2));

    assertThat(matrix[0][0], is(2L));
    assertThat(matrix[0][1], is(4L));
    assertThat(matrix[1][0], is(2L));
    assertThat(matrix[1][1], is(6L));

    assertThat(mergedResult[0][0], is(2L));
    assertThat(mergedResult[0][1], is(4L));
    assertThat(mergedResult[1][0], is(2L));
    assertThat(mergedResult[1][1], is(6L));
  }

  // https://scikit-learn.org/stable/modules/generated/sklearn.metrics.confusion_matrix.html
  @Test
  public void multiclass_classification_string_labels() {
    val profile = new DatasetProfile("session", Instant.now());

    val metrics = ClassificationMetrics.of();
    val predictions = ImmutableList.of("cat", "ant", "cat", "cat", "ant", "bird");
    val targets = ImmutableList.of("ant", "ant", "cat", "cat", "ant", "cat");
    Streams.zip(predictions.stream(), targets.stream(), Pair::of)
        .forEach(pair -> metrics.update(profile, pair.getLeft(), pair.getRight(), 0));
    val matrix = metrics.getConfusionMatrix();
    assertThat(matrix.length, is(3));
    assertThat(matrix[0].length, is(3));
    assertThat(matrix[1].length, is(3));
    assertThat(matrix[2].length, is(3));

    // Result matrix
    // array([[2, 0, 0],
    //       [0, 0, 1],
    //       [1, 0, 2]])
    assertThat(matrix[0][0], is(2L));
    assertThat(matrix[0][1], is(0L));
    assertThat(matrix[0][2], is(0L));
    assertThat(matrix[1][0], is(0L));
    assertThat(matrix[1][1], is(0L));
    assertThat(matrix[1][2], is(1L));
    assertThat(matrix[2][0], is(1L));
    assertThat(matrix[2][1], is(0L));
    assertThat(matrix[2][2], is(2L));
  }

  // https://scikit-learn.org/stable/modules/generated/sklearn.metrics.confusion_matrix.html
  @Test
  public void multiclass_classification_integer_labels() {
    val profile = new DatasetProfile("session", Instant.now());

    val metrics = ClassificationMetrics.of();
    val predictions = ImmutableList.of(2, 0, 2, 2, 0, 1);
    val targets = ImmutableList.of(0, 0, 2, 2, 0, 2);
    Streams.zip(predictions.stream(), targets.stream(), Pair::of)
        .forEach(pair -> metrics.update(profile, pair.getLeft(), pair.getRight(), 0));
    val matrix = metrics.getConfusionMatrix();
    assertThat(matrix.length, is(3));
    assertThat(matrix[0].length, is(3));
    assertThat(matrix[1].length, is(3));
    assertThat(matrix[2].length, is(3));

    // Result matrix
    // array([[2, 0, 0],
    //       [0, 0, 1],
    //       [1, 0, 2]])
    assertThat(matrix[0][0], is(2L));
    assertThat(matrix[0][1], is(0L));
    assertThat(matrix[0][2], is(0L));
    assertThat(matrix[1][0], is(0L));
    assertThat(matrix[1][1], is(0L));
    assertThat(matrix[1][2], is(1L));
    assertThat(matrix[2][0], is(1L));
    assertThat(matrix[2][1], is(0L));
    assertThat(matrix[2][2], is(2L));
  }

  @Test
  public void multiclass_classification_roundtrip() {
    val profile = new DatasetProfile("session", Instant.now());

    val metrics = ClassificationMetrics.of();
    val predictions = ImmutableList.of(2, 0, 2, 2, 0, 1);
    val targets = ImmutableList.of(0, 0, 2, 2, 0, 2);
    Streams.zip(predictions.stream(), targets.stream(), Pair::of)
        .forEach(pair -> metrics.update(profile, pair.getLeft(), pair.getRight(), 0));

    val matrix = metrics.getConfusionMatrix();

    val msg = metrics.toProtobuf();
    val roundtrip = ClassificationMetrics.fromProtobuf(msg.build());
    val rtMatrix = roundtrip.getConfusionMatrix();
    for (int i = 0; i < 3; i++) {
      for (int j = 0; j < 3; j++) {
        assertThat(rtMatrix[i][j], is(matrix[i][j]));
      }
    }
    // note that the roundtrip object now contains labels in string format
    assertThat(roundtrip.getLabels(), containsInRelativeOrder("0", "1", "2"));
  }
}
