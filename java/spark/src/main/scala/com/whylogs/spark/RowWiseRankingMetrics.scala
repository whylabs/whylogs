/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Contents of this file were adapted from https://github.com/apache/spark/blob/master/mllib/src/main/scala/org/apache/spark/mllib/evaluation/RankingMetrics.scala
 * Summary of changes:
 *  - Changed RankingMetrics to object RowWiserRankingMetrics
 *  - removed RDD and made functions row wise operations on tuples
 */


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
        relevantItemsRetrieved.toDouble / k // should we use convention of n here
    }
  }

    /**
  * Computes the precision at first k ranking positions for a single query.
  *
  * @param predictionRanks Array of predicted ranking positions
  * @param binaryLabels Array of binary ground truth relevance labels
  * @param k Use the top k predicted rankings, must be positive
  * @return Precision at first k ranking positions
  */
  def precisionAtKBinary(predictionRanks: Array[Int], binaryLabels: Array[Boolean], k: Int): Double = {
    require(k > 0, "The ranking position k must be positive")
    // Check for empty arrays and return 0.0
    if (predictionRanks.isEmpty || binaryLabels.isEmpty) {
      logWarning("Empty ground truth set, check input data")
      0.0
    } else {

      val pairedData = predictionRanks.zip(binaryLabels)

      // Filter pairs where rank is less than or equal to k, then count relevant items
      val predictionsUnderK = pairedData.filter { case (rank, _) => rank <= k }
      val relevantCount = predictionsUnderK.count { case (_, binaryLabel) => binaryLabel }
      relevantCount.toDouble / k
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
   * Computes the average precision at first k ranking positions for a single query.
   *
   * @param predictions Array of predicted rankings
   * @param labels Array of ground truth labels
   * @param k Use the top k predicted rankings, must be positive
   * @return Average precision at first k ranking positions
   */
  def averagePrecisionAtKBinary(predictionRanks: Array[Int], binaryLabels: Array[Boolean], k: Int): Double = {
    require(k > 0, "The ranking position k must be positive")

    if (predictionRanks.isEmpty || binaryLabels.isEmpty) {
      logWarning("Empty ground truth set, check input data")
      0.0
    } else {
      val pairedData = predictionRanks.zip(binaryLabels)
      val predictionsUnderK = pairedData.filter { case (rank, _) => rank <= k }

      if (predictionsUnderK.isEmpty) {
        logWarning("There are no labels for ranks under k, returning 0 as default")
        0.0
      } else {
        var count = 0
        var precisionSum = 0.0
        val trueCount = binaryLabels.count(_ == true)
        if (trueCount == 0) {
          0.0
        } else {
          for ((rank, relevant) <- predictionsUnderK) {
            if (relevant) {
              count += 1
              precisionSum += (count.toDouble / rank.toDouble) 
            }
          }
          precisionSum / math.min(trueCount, k)
        }
      }
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
   * Computes the recall at first k ranking positions for a single query.
   *
   * @param predictionRanks Array of predicted rankings
   * @param binaryLabels Array of ground truth labels, the length of the binary labels is the max relevant items
   * @param k Use the top k predicted rankings, must be positive
   * @return Recall at first k ranking positions
   */
  def recallAtKBinary(predictionRanks: Array[Int], binaryLabels: Array[Boolean], k: Int): Double = {
    require(k > 0, "The ranking position k must be positive")

    if (binaryLabels.isEmpty || binaryLabels.count(_ == true) == 0) {
      logWarning("Empty ground truth set, check input data")
      0.0
    } else {
      val trueCount = binaryLabels.count(_ == true)
      if (trueCount == 0) {
        0.0
      } else {
        val pairedData = predictionRanks.zip(binaryLabels)
        val predictionsUnderK = pairedData.filter { case (rank, _) => rank <= k }
        val relevantCount = predictionsUnderK.count { case (_, binaryLabel) => binaryLabel }
        relevantCount.toDouble / trueCount
      }
    }
  }

  /**
   * Computes the NDCG (Normalized Discounted Cumulative Gain) at first k ranking positions for a single query.
   *
   * @param predictionRanks Array of predicted rankings
   * @param labels Array of ground truth relevance labels
   * @param k Use the top k predicted rankings, must be positive
   * @return NDCG at first k ranking positions
   */
  def ndcgAtKBinary(predictionRanks: Array[Int], binaryLabels: Array[Boolean], k: Int): Double = {
    require(k > 0, "The ranking position k must be positive")

    if (predictionRanks.isEmpty || binaryLabels.isEmpty) {
      logWarning("Empty predictions or ground truth set, check input data")
      0.0
    } else {
      // Using the length of the smaller of the two arrays or k to prevent index out of bounds
      val trueCount = binaryLabels.count(_ == true)
      if (trueCount == 0) {
        0.0
      } else {
        val n = math.min(math.min(predictionRanks.length, trueCount), k)
        var dcg = 0.0
        var maxDcg = 0.0

        for (i <- 0 until n) {
          // Base of the log doesn't matter for calculating NDCG,
          // if the relevance value is binary.
          val relevant = binaryLabels(i)
          val gain = if (relevant) 1.0 / math.log(i + 2) else 0.0

          dcg += gain
          // Every element is considered relevant for maxDcg until k
          maxDcg += 1.0 / math.log(i + 2)
        }

        if (maxDcg == 0.0) {
          logWarning("Maximum of relevance of ground truth set is zero, check input data")
          0.0
        } else {
          dcg / maxDcg
        }
      }
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
    if (predictions.isEmpty) {
      logWarning("Empty predictions set, check input data")
      return 0.0
    }
    if (labels.isEmpty) {
      logWarning("Empty ground truth, check input data")
      return 0.0
    }

    val labelSet = labels.toSet
    val useBinary = relevanceValues.isEmpty
    val relevanceMap = labels.zip(relevanceValues).toMap


    val labSetSize = labelSet.size
    val n = math.min(math.max(predictions.length, labSetSize), k)
    var dcg = 0.0
    var maxDcg = 0.0
    var i = 0
    while (i < n) {
      if (useBinary) {
          // Base of the log doesn't matter for calculating NDCG,
          // if the relevance value is binary.
          val gain = 1.0 / math.log(i + 2)
          if (i < predictions.length && labelSet.contains(predictions(i))) {
            dcg += gain
          }
          if (i < labSetSize) {
            maxDcg += gain
          }
      } else {
        if (i < predictions.length) {
          dcg += (math.pow(2.0, relevanceMap.getOrElse(predictions(i), 0.0)) - 1) / math.log(i + 2)
        }
        if (i < labSetSize) {
          maxDcg += (math.pow(2.0, relevanceMap.getOrElse(labels(i), 0.0)) - 1) / math.log(i + 2)
        }
      }
      i += 1
    }

    if (maxDcg == 0.0) {
      logWarning("Maximum of relevance of ground truth set is zero, check input data")
      0.0
    } else {
      dcg / maxDcg
    }
  }

}
