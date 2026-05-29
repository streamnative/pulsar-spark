/*
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

import org.apache.spark.SparkFunSuite

class PulsarExceptionsSuite extends SparkFunSuite {

  // Regression guard for AbstractMethodError on Spark distributions that leave the
  // deprecated SparkThrowable.getErrorClass abstract (e.g. Databricks Runtime). The
  // connector exceptions must declare their OWN getErrorClass body; relying on the
  // interface default causes Spark's TaskResultGetter to raise AbstractMethodError
  // when the exception escapes a task, which silently hangs the job.

  test("PulsarIllegalStateException declares its own getErrorClass") {
    val e = PulsarExceptions.pulsarSinkIncompatibleSchema(
      "persistent://public/default/t", new RuntimeException("boom"))
    assert(e.getCondition === "PULSAR_SINK_INCOMPATIBLE_SCHEMA")
    // getDeclaredMethod throws NoSuchMethodException if the body is not on the class
    // itself (i.e. if it falls back to the SparkThrowable default).
    val method = classOf[PulsarIllegalStateException].getDeclaredMethod("getErrorClass")
    assert(method.invoke(e) === "PULSAR_SINK_INCOMPATIBLE_SCHEMA")
  }

  test("PulsarIllegalArgumentException declares its own getErrorClass") {
    val e = PulsarExceptions.pulsarSinkInvalidSchema
    assert(e.getCondition === "PULSAR_SINK_INVALID_SCHEMA")
    val method = classOf[PulsarIllegalArgumentException].getDeclaredMethod("getErrorClass")
    assert(method.invoke(e) === "PULSAR_SINK_INVALID_SCHEMA")
  }
}
