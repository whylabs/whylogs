package com.whylogs.spark

import org.apache.spark.sql.{DataFrame, functions => F}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DoubleType
import scala.reflect.runtime.universe.TypeTag
import scala.reflect.ClassTag


class RankingMetricsUDF[T : TypeTag : ClassTag](predictionField: String, targetField: String, scoreField: String = null, k: Int = 10)  extends Serializable {

  // Define typed UDF for sequences without scores
  def udfWithSeq[T: TypeTag : ClassTag](f: (Seq[T], Seq[T]) => Option[Double]): UserDefinedFunction =
    F.udf((predictions: Seq[T], labels: Seq[T]) =>
      if (predictions == null || labels == null || predictions.isEmpty || labels.isEmpty) {
        Option.empty[Double]
      } else {
        f(predictions, labels)
      }
    )

  // Define typed UDF for sequences with scores
  def udfWithSeqAndScores[T: TypeTag : ClassTag](f: (Seq[T], Seq[T], Seq[Double]) => Option[Double]): UserDefinedFunction =
    F.udf((predictions: Seq[T], labels: Seq[T], scores: Seq[Double]) =>
      if (predictions == null || labels == null || predictions.isEmpty || labels.isEmpty) {
        Option.empty[Double]
      } else {
        val safeScores = Option(scores).getOrElse(Seq.empty)
        f(predictions, labels, safeScores)
      }
    )

  val averagePrecisionAtK: UserDefinedFunction = udfWithSeq[T]((predictions, labels) =>
    Option(RowWiseRankingMetrics.averagePrecisionAtK(predictions.toArray, labels.toArray, k)))

  val precisionAtKUDF: UserDefinedFunction = udfWithSeq[T]((predictions, labels) =>
    Option(RowWiseRankingMetrics.precisionAtK(predictions.toArray, labels.toArray, k)))

  val recallAtKUDF: UserDefinedFunction = udfWithSeq[T]((predictions, labels) =>
    Option(RowWiseRankingMetrics.recallAtK(predictions.toArray, labels.toArray, k)))

  val ndcgAtKUDF: UserDefinedFunction = udfWithSeqAndScores[T]((predictions, labels, scores) =>
    Option(RowWiseRankingMetrics.ndcgAtK(predictions.toArray, labels.toArray, scores.toArray, k)))

 def applyMetrics(df: DataFrame, withNDCG: Boolean = true): DataFrame = {
    val baseDf = df
      .withColumn(s"precision_k_$k", precisionAtKUDF(F.col(predictionField), F.col(targetField)))
      .withColumn(s"average_precision_k_$k", averagePrecisionAtK(F.col(predictionField), F.col(targetField)))
      .withColumn(s"recall_k_$k", recallAtKUDF(F.col(predictionField), F.col(targetField)))
    if (withNDCG) {
      if (scoreField != null && df.columns.contains(scoreField)) {
        baseDf.withColumn(s"norm_dis_cumul_gain_k_$k", ndcgAtKUDF(F.col(predictionField), F.col(targetField), F.col(scoreField)))
      } else {
        baseDf.withColumn(s"norm_dis_cumul_gain_k_$k", ndcgAtKUDF(F.col(predictionField), F.col(targetField), F.lit(Array.empty[Double])))
      }
    } else {
      baseDf
    }
  }
}
