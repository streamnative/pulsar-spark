/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.pulsar

import java.security.SecureRandom

import scala.util.{Failure, Success, Try}

trait Retrier {
  def retry[T](f: => T): T
}

object SimpleRetrier {
  val DoNotRetry = 0
}

class SimpleRetrier(maxRetries: Int,
                    waitTimeMs: Int,
                    multiplier: Double,
                    randomizationFactor: Double) extends Retrier {

  val secRandom = new SecureRandom()

  override def retry[T](retriable: => T): T = retryRec(maxRetries,
    List[Throwable](), List[Int](waitTimeMs))(retriable)

  private def retryRec[T](remainingAttempts: Int,
                          exceptions: List[Throwable],
                          waitTimes: List[Int])(retriable: => T): T = {
    Try {
      retriable
    } match {
      case Success(value) =>
        value
      case Failure(e) =>
        if (remainingAttempts > 0) {
          val waitTime = waitTimes.last
          Thread.sleep(waitTime)
          val nextWaitTime = calculateNextRetry(waitTime)
          retryRec(remainingAttempts - 1,
            exceptions :+ e, waitTimes :+ nextWaitTime)(retriable)
        } else {
          throw new RuntimeException(s"Call failed. " +
            s"Attempted call ${maxRetries + 1} times." +
            s"Retry wait times: ${waitTimes.map(_.toString).mkString(",")} " +
            s"Exceptions during retry: ${exceptions.map(_.toString).mkString("\n")}. " +
            s"Final exception:", e)
        }
    }
  }

  private def calculateNextRetry(previousRetry: Int) = {
    val randomFactor = secRandom.nextFloat() * randomizationFactor + 1.0
    (previousRetry * randomFactor * multiplier).toInt
  }
}
