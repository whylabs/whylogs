package com.whylogs.spark

import scala.reflect.ClassTag
import scala.collection.immutable
import org.apache.spark.internal.Logging

object RowWiseRankingMetrics extends Logging {

  def toMap[K, V](keys: Iterable[K], values: Iterable[V]): Map[K, V] = {
    val builder = immutable.Map.newBuilder[K, V]
    val keyIter = keys.iterator
    val valueIter = values.iterator
    while (keyIter.hasNext && valueIter.hasNext) {
      builder += (keyIter.next(), valueIter.next()).asInstanceOf[(K, V)]
    }
    builder.result()
  }
    /**
    * Computes the precision at first k ranking positions for a single query.
    *
    * @param predictions Array of predicted rankings
    * @param labels Array of ground truth labels
    * @param k Use the top k predicted rankings, must be positive
    * @return Precision at first k ranking positions
    */
    def precisionAtK[T](predictions: Array[T], labels: Array[T], k: Int): Double = {
    require(k > 0, "The ranking position k must be positive")
    val labelSet = labels.toSet
    if (labelSet.isEmpty) {
        logWarning("Empty ground truth set, check input data")
        0.0
    } else {
        val n = math.min(predictions.length, k)
        val relevantItemsRetrieved = predictions.slice(0, n).count(labelSet.contains)
        relevantItemsRetrieved.toDouble / k
    }
    }

  /**
   * Computes the average precision at first k ranking positions for a single query.
   *
   * @param predictions Array of predicted rankings
   * @param labels Array of ground truth labels
   * @param k Use the top k predicted rankings, must be positive
   * @return Average precision at first k ranking positions
   */
  def averagePrecisionAtK[T](predictions: Array[T], labels: Array[T], k: Int): Double = {
    require(k > 0, "The ranking position k must be positive")
    val labelSet = labels.toSet
    if (labelSet.isEmpty) {
      logWarning("Empty ground truth set, check input data")
      0.0
    } else {
      var i = 0
      var count = 0
      var precisionSum = 0.0
      val n = math.min(k, predictions.length)
      while (i < n) {
        if (labelSet.contains(predictions(i))) {
          count += 1
          precisionSum += count.toDouble / (i + 1)
        }
        i += 1
      }
      precisionSum / math.min(labelSet.size, k)
    }
  }

  /**
   * Computes the recall at first k ranking positions for a single query.
   *
   * @param predictions Array of predicted rankings
   * @param labels Array of ground truth labels
   * @param k Use the top k predicted rankings, must be positive
   * @return Recall at first k ranking positions
   */
  def recallAtK[T](predictions: Array[T], labels: Array[T], k: Int): Double = {
    require(k > 0, "The ranking position k must be positive")
    val labelSet = labels.toSet
    if (labelSet.isEmpty) {
      logWarning("Empty ground truth set, check input data")
      0.0
    } else {
      val n = math.min(predictions.length, k)
      val relevantItemsRetrieved = predictions.slice(0, n).count(labelSet.contains)
      relevantItemsRetrieved.toDouble / labelSet.size
    }
  }

  /**
   * Computes the NDCG (Normalized Discounted Cumulative Gain) at first k ranking positions for a single query.
   *
   * @param predictions Array of predicted rankings
   * @param labels Array of ground truth labels
   * @param relevanceValues Array of relevance values corresponding to each label
   * @param k Use the top k predicted rankings, must be positive
   * @return NDCG at first k ranking positions
   */
  def ndcgAtK[T](predictions: Array[T], labels: Array[T], relevanceValues: Array[Double], k: Int): Double = {
    require(k > 0, "The ranking position k must be positive")
    if (labels.isEmpty || predictions.isEmpty) {
      logWarning("Empty ground truth or predictions set, check input data")
      return 0.0
    }

    val labelSet = labels.toSet
    val relevanceMap = labels.zip(relevanceValues).toMap
    var dcg = 0.0
    var maxDcg = 0.0

    // Use the minimum of k, predictions.length, and labels.length to avoid out-of-bounds issues
    val n = math.min(math.min(k, predictions.length), labels.length)
    for (i <- 0 until n) {
      // Only access predictions(i) and labels(i) within their valid indices
      if (labelSet.contains(predictions(i))) {
        val gain = relevanceMap.get(predictions(i)).map(rel => (math.pow(2.0, rel) - 1) / math.log(i + 2)).getOrElse(0.0)
        dcg += gain
      }
      // Safe to access labels(i) because of the loop limit
      val maxGain = relevanceMap.get(labels(i)).map(rel => (math.pow(2.0, rel) - 1) / math.log(i + 2)).getOrElse(0.0)
      maxDcg += maxGain
    }

    if (maxDcg == 0.0) {
      logWarning("Maximum of relevance of ground truth set is zero, check input data")
      0.0
    } else {
      dcg / maxDcg
    }
  }


}
