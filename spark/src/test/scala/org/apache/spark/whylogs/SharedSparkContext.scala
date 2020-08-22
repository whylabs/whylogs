package org.apache.spark.whylogs

import java.time.LocalDateTime
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.sql.SparkSession
import SharedSparkContext.{_spark, activeTestSuitesCount, lock}
import org.scalatest.{BeforeAndAfterAll, Suite}

object SharedSparkContext {
  // prevent two test suites from modify the Spark session
  private val lock = new ReentrantLock()

  // we can only stop the Spark session if this value is 0
  @transient private var activeTestSuitesCount = 0

  // the current Spark session
  @transient private var _spark: SparkSession = _
}

trait SharedSparkContext extends BeforeAndAfterAll {
  self: Suite =>

  def spark: SparkSession = _spark

  override def beforeAll() {
    super.beforeAll()

    try {
      lock.lock()

      activeTestSuitesCount += 1
      if (_spark == null) {
        val builder = SparkSession.builder()
          .master("local[*, 3]")
          .appName("SparkTesting-" + LocalDateTime.now().toString)
          .config("spark.ui.enabled", "false")

        _spark = builder.getOrCreate()
      }
    } finally {
      lock.unlock()
    }
  }

  override def afterAll: Unit = {
    super.afterAll()

    try {
      lock.lock()
      if (activeTestSuitesCount <= 0 && _spark != null) {
        _spark.stop()
        _spark = null
      }
    } finally {
      lock.unlock()
    }
  }
}
