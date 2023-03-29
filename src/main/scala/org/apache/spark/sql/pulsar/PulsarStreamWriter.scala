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

import java.{util => ju}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.connector.write.{
  DataWriter,
  DataWriterFactory,
  PhysicalWriteInfo,
  WriterCommitMessage
}
import org.apache.spark.sql.connector.write.streaming.{StreamingDataWriterFactory, StreamingWrite}
import org.apache.spark.sql.types.StructType

/**
 * Dummy commit message. The DataSourceV2 framework requires a commit message implementation but
 * we don't need to really send one.
 */
case object PulsarWriterCommitMessage extends WriterCommitMessage

/**
 * A [[StreamWriter]] for Pulsar writing. Responsible for generating the writer factory.
 *
 * @param schema
 *   The schema of the input data.
 * @param clientConf
 *   Parameters for Pulsar client in each task.
 * @param producerConf
 *   Parameters for Pulsar producers in each task.
 * @param topic
 *   The topic this writer is responsible for.
 */
class PulsarStreamWriter(
    schema: StructType,
    clientConf: ju.Map[String, Object],
    producerConf: ju.Map[String, Object],
    topic: Option[String],
    adminUrl: String)
    extends StreamingWrite {

  override def createStreamingWriterFactory(info: PhysicalWriteInfo): StreamingDataWriterFactory =
    new PulsarStreamWriterFactory(schema, clientConf, producerConf, topic, adminUrl)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

/**
 * A [[DataWriterFactory]] for Pulsar writing. Will be serialized and sent to executors to
 * generate the per-task data writers.
 * @param schema
 *   The schema of the input data.
 * @param clientConf
 *   Parameters for Pulsar client.
 * @param producerConf
 *   Parameters for Pulsar producers in each task.
 * @param topic
 *   The topic that should be written to.
 */
class PulsarStreamWriterFactory(
    schema: StructType,
    clientConf: ju.Map[String, Object],
    producerConf: ju.Map[String, Object],
    topic: Option[String],
    adminUrl: String)
    extends StreamingDataWriterFactory {

  override def createWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new PulsarStreamDataWriter(schema.toAttributes, clientConf, producerConf, topic, adminUrl)
  }
}

/**
 * A [[DataWriter]] for Pulsar writing. One data writer will be created in each partition to
 * process incoming rows.
 *
 * @param topic
 *   The topic that this data writer is targeting.
 * @param clientConf
 *   Parameters to use for the Pulsar client.
 * @param producerConf
 *   Parameters to use for the Pulsar producer.
 * @param inputSchema
 *   The attributes in the input data.
 */
class PulsarStreamDataWriter(
    inputSchema: Seq[Attribute],
    clientConf: ju.Map[String, Object],
    producerConf: ju.Map[String, Object],
    topic: Option[String],
    adminUrl: String)
    extends PulsarRowWriter(inputSchema, clientConf, producerConf, topic, adminUrl)
    with DataWriter[InternalRow] {

  def write(row: InternalRow): Unit = {
    checkForErrors()
    sendRow(row)
  }

  def commit(): WriterCommitMessage = {
    // Send is asynchronous, but we can't commit until all rows are actually in Pulsar.
    // This requires flushing and then checking that no callbacks produced errors.
    // We also check for errors before to fail as soon as possible - the check is cheap.
    checkForErrors()
    producerFlush()
    checkForErrors()
    PulsarWriterCommitMessage
  }

  def abort(): Unit = {}

  def close(): Unit = {
    checkForErrors()
    producerClose()
    checkForErrors()
  }
}
