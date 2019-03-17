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

import java.{util => ju}

import org.apache.pulsar.client.api.Message
import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.types.{BinaryType, StringType}
import org.apache.spark.util.Utils

/**
  * The [[PulsarWriter]] class is used to write data from a batch query
  * or structured streaming query, given by a [[QueryExecution]], to Kafka.
  * The data is assumed to have a value column, and an optional topic and key
  * columns. If the topic column is missing, then the topic must come from
  * the 'topic' configuration option. If the key column is missing, then a
  * null valued key field will be added to the
  * [[Message]].
  */
private[pulsar] object PulsarWriter {

  override def toString: String = "PulsarWriter"

  def validateQuery(schema: Seq[Attribute]): Unit = {
    schema.find(_.name == PulsarOptions.KEY_ATTRIBUTE_NAME).getOrElse(
      Literal(null, StringType)
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(s"${PulsarOptions.KEY_ATTRIBUTE_NAME} attribute type " +
          s"must be a ${StringType.catalogString} or ${BinaryType.catalogString}")
    }
    schema.find(_.name == PulsarOptions.VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new AnalysisException(s"Required attribute '${PulsarOptions.VALUE_ATTRIBUTE_NAME}' not found")
    ).dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(s"${PulsarOptions.VALUE_ATTRIBUTE_NAME} attribute type " +
          s"must be a ${StringType.catalogString} or ${BinaryType.catalogString}")
    }
  }

  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      pulsarClientConf: ju.Map[String, Object],
      pulsarProducerConf: ju.Map[String, Object],
      topic: String): Unit = {

    // validate the schema
    val schema = queryExecution.analyzed.output
    validateQuery(schema)

    // execute RDD
    queryExecution.toRdd.foreachPartition { iter =>
      val writeTask = new PulsarWriteTask(
        pulsarClientConf,
        pulsarProducerConf,
        topic,
        schema)
      Utils.tryWithSafeFinally(block = writeTask.execute(iter))(
        finallyBlock = writeTask.close())
    }
  }

}
