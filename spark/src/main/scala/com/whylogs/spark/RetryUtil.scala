package com.whylogs.spark

import org.slf4j.LoggerFactory

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

object RetryUtil {
  private val logger = LoggerFactory.getLogger(getClass)

  case class RetryConfig(maxTries: Int, initialWaitMillis: Int)

  protected case class RetryContext(retries: Int, lastWaitMillis: Int, lastCause: Throwable = null)

  class PermanentFailure(msg: String, cause: Throwable) extends RuntimeException(msg, cause)

  def withRetries[T](config: RetryConfig = RetryConfig(3, 1000))(work: => T): Future[T] = {
    _withRetries(config, RetryContext(1, config.initialWaitMillis), work)
  }


  private def _withRetries[T](config: RetryConfig, context: RetryContext, work: => T): Future[T] = {
    logger.debug(
      s"""
         |Starting retry attempt ${context.retries}
         |Backoff at ${context.lastWaitMillis}ms
         |""".stripMargin)

    Future {
      work
    }.recoverWith {
      case t: Throwable =>
        if (context.retries >= config.maxTries) {
          throw new PermanentFailure("Failed too many times.", context.lastCause)
        }
        completeAfter(context.lastWaitMillis)
          .flatMap { _ =>
            _withRetries(config, RetryContext(context.retries + 1, context.lastWaitMillis * 2, t), work)
          }
    }
  }

  private def completeAfter(millis: Int): Future[Unit] = {
    Future {
      Thread.sleep(millis)
    }
  }
}
