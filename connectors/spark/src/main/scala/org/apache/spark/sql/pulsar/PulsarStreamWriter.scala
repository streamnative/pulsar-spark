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

import java.util.concurrent.TimeUnit
import java.{util => ju}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.pulsar.PulsarWriter.validateQuery
import org.apache.spark.sql.sources.v2.writer.{DataWriter, DataWriterFactory, WriterCommitMessage}
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.types.StructType

/**
 * Dummy commit message. The DataSourceV2 framework requires a commit message implementation but we
 * don't need to really send one.
 */
case object PulsarWriterCommitMessage extends WriterCommitMessage

/**
 * A [[StreamWriter]] for Pulsar writing. Responsible for generating the writer factory.
 *
 * @param topic The topic this writer is responsible for.
 * @param producerParams Parameters for Pulsar producers in each task.
 * @param schema The schema of the input data.
 */
class PulsarStreamWriter(
    schema: StructType,
    pulsarClientConf: ju.Map[String, Object],
    pulsarProducerConf: ju.Map[String, Object],
    topic: String)
  extends StreamWriter {

  validateQuery(schema.toAttributes)

  override def createWriterFactory(): PulsarStreamWriterFactory =
    PulsarStreamWriterFactory(schema, pulsarClientConf, pulsarProducerConf, topic)

  override def commit(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
  override def abort(epochId: Long, messages: Array[WriterCommitMessage]): Unit = {}
}

/**
 * A [[StreamingDataWriterFactory]] for Pulsar writing. Will be serialized and sent to executors to
 * generate the per-task data writers.
 * @param topic The topic that should be written to.
 * @param producerParams Parameters for Pulsar producers in each task.
 * @param schema The schema of the input data.
 */
case class PulsarStreamWriterFactory(
    schema: StructType,
    clientParams: ju.Map[String, Object],
    producerParams: ju.Map[String, Object],
    topic: String)
  extends DataWriterFactory[InternalRow] {

  override def createDataWriter(
      partitionId: Int,
      taskId: Long,
      epochId: Long): DataWriter[InternalRow] = {
    new PulsarStreamDataWriter(
      schema.toAttributes,
      clientParams,
      producerParams,
      topic
    )
  }
}

/**
 * A [[DataWriter]] for Pulsar writing. One data writer will be created in each partition to
 * process incoming rows.
 *
 * @param targetTopic The topic that this data writer is targeting.
 * @param producerParams Parameters to use for the Pulsar producer.
 * @param inputSchema The attributes in the input data.
 */
class PulsarStreamDataWriter(
    inputSchema: Seq[Attribute],
    pulsarClientConf: ju.Map[String, Object],
    pulsarProducerConf: ju.Map[String, Object],
    topic: String)
  extends PulsarRowWriter(inputSchema) with DataWriter[InternalRow] {

  lazy val producer = CachedPulsarClient.getOrCreate(pulsarClientConf)
    .newProducer(org.apache.pulsar.client.api.Schema.BYTES)
    .topic(topic)
    .loadConf(pulsarProducerConf)
    .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
    // maximizing the throughput
    .batchingMaxMessages(5 * 1024 * 1024)
    .create();

  def write(row: InternalRow): Unit = {
    checkForErrors()
    sendRow(row, producer)
  }

  def commit(): WriterCommitMessage = {
    // Send is asynchronous, but we can't commit until all rows are actually in Pulsar.
    // This requires flushing and then checking that no callbacks produced errors.
    // We also check for errors before to fail as soon as possible - the check is cheap.
    checkForErrors()
    producer.flush()
    checkForErrors()
    PulsarWriterCommitMessage
  }

  def abort(): Unit = {}

  def close(): Unit = {
    checkForErrors()
    if (producer != null) {
      producer.flush()
      checkForErrors()
      producer.close()
    }
  }
}
