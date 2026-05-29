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
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.types.{BinaryType, StringType}

class PulsarSinkValidateQuerySuite extends SparkFunSuite {

  private val topic = Some("persistent://public/default/t")

  test("validateQuery rejects a schema with no value fields") {
    // Only a reserved meta field (__key) is present, so there is no value column
    // to write. validateQuery must throw a PulsarIllegalArgumentException carrying
    // PULSAR_SINK_INVALID_SCHEMA_TYPE. Previously this raised INTERNAL_ERROR (the
    // error class did not match error/pulsar-error-classes.json) and the exception
    // was never actually thrown (missing `throw`), so the check was a no-op.
    val schema = Seq(AttributeReference(PulsarOptions.KeyAttributeName, BinaryType)())
    val e = intercept[PulsarIllegalArgumentException] {
      PulsarSinks.validateQuery(schema, topic)
    }
    assert(e.getCondition === "PULSAR_SINK_INVALID_SCHEMA_TYPE")
  }

  test("validateQuery accepts a schema with at least one value field") {
    val schema = Seq(
      AttributeReference(PulsarOptions.KeyAttributeName, BinaryType)(),
      AttributeReference("value", StringType)())
    // Should not throw.
    PulsarSinks.validateQuery(schema, topic)
  }
}
