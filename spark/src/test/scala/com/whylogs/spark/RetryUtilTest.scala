package com.whylogs.spark

import org.scalatest.funsuite.AnyFunSuite

import scala.concurrent.Await
import scala.concurrent.duration.Duration

class RetryUtilTest extends AnyFunSuite {

  test("test happy path") {
    var attempts = 0

    val result = RetryUtil.withRetries() {
      attempts += 1
      "done"
    }

    Await.ready(result, Duration.Inf)
    assert(attempts == 1)
  }

  test("permanent failures happens when it should") {
    var attempts = 0

    val result = RetryUtil.withRetries(RetryUtil.RetryConfig(3, 10)) {
      attempts += 1
      throw new RuntimeException("error")
    }

    try {
      Await.result(result, Duration.Inf)
    } catch {
      case e: RetryUtil.PermanentFailure => {
        assert(attempts == 3)
        assert(e.getCause.isInstanceOf[RuntimeException])
      }
      case default => throw new RuntimeException("Wrong error occurred")
    }
  }
}

