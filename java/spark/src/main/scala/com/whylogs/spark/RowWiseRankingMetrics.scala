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
