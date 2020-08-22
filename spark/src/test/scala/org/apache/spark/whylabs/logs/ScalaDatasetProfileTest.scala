package org.apache.spark.whylabs.logs

import java.time.Instant

import com.whylogs.core.DatasetProfile
import org.scalatest.funsuite.AnyFunSuite


class ScalaDatasetProfileTest extends AnyFunSuite with SharedSparkContext {
  test("SparkDatasetProfileUDT") {
    val _spark = spark
    import _spark.implicits._

    // verify we can have a Dataset of ScalaDatasetProfile
    val profileDs = (1 to 100)
      .map(i => new DatasetProfile(s"tes-$i", Instant.now()))
      .map(ds => ScalaDatasetProfile(ds))
      .toDS()

    val endsWithZeros = profileDs.repartition(32)
      .filter(ds => {
        ds.value.getSessionId.endsWith("0")
      })
      .map(ds => ds.value.getSessionId)
      .count()
    assert(endsWithZeros == 10)
  }
}
