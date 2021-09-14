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

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkFunSuite

class SimpleRetrierSuite extends SparkFunSuite {
  test("shall call provided method") {
    val testRetrier = new SimpleRetrier(SimpleRetrier.DoNotRetry, 1000, 1.5, 0.5)
    var called = false
    testRetrier.retry{
      called = true
    }
    assert(called == true)
  }

  test("shall fail when it only tries once and fail") {
    val testRetrier = new SimpleRetrier(SimpleRetrier.DoNotRetry, 1000, 1.5, 0.5)
    var expectedThrowable: Throwable = null
    val exceptionThrown = Try {
      testRetrier.retry{
        throw new RuntimeException("test exception")
      }
    } match {
      case Success(_) => false
      case Failure(_) => true
    }
    assert(exceptionThrown == true)
  }

  test("shall fail when maximum tries are exhausted") {
    val testRetrier = new SimpleRetrier(10, 50, 1.5, 0.5)
    var tries = 0
    val failedWithException = Try {
      testRetrier.retry{
        tries += 1
        throw new RuntimeException("test exception")
      }
    } match {
      case Success(_) => false
      case Failure(_) => true
    }
    assert(failedWithException == true, "Exception was not thrown after " +
      "constantly failing retry")
    assert(tries == 11)
  }

  test("shall pass when call succeeds after failures") {
    val testRetrier = new SimpleRetrier(10, 50, 1.5, 0.5)
    val succeedAfterTry = 5
    var tries = 0
    testRetrier.retry{
      tries += 1
      if (tries < succeedAfterTry) {
        throw new RuntimeException("test exception")
      }
    }
    assert(tries < 10)
  }
}
