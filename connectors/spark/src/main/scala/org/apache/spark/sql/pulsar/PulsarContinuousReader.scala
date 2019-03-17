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

import org.apache.pulsar.client.api.{Message, MessageId}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.sources.v2.reader.{ContinuousInputPartition, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputPartitionReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType

/**
 * A [[ContinuousReader]] for reading data from Pulsar.
  *
 * @param pulsarParams
 * @param sourceOptions
 * @param initialMessageIds
 */
class PulsarContinuousReader(
    pulsarClientConf: ju.Map[String, Object],
    pulsarReaderConf: ju.Map[String, Object],
    initialOffset: PulsarSourceOffset)
  extends ContinuousReader with Logging {

  private lazy val session = SparkSession.getActiveSession.get
  private lazy val sc = session.sparkContext

  @volatile private[sql] var knownTopics: Set[String] = _

  override def readSchema(): StructType = PulsarReader.pulsarSchema

  private var offset: Offset = _
  override def setStartOffset(start: ju.Optional[Offset]): Unit = {
    offset = start.orElse {
      logInfo(s"Initial offsets : $initialOffset")
      initialOffset
    }
  }
  override def getStartOffset: Offset = offset

  override def deserializeOffset(json: String): Offset = {
    PulsarSourceOffset(JsonUtils.topicOffsets(json))
  }

  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    import scala.collection.JavaConverters._

    val topicOffsets = PulsarSourceOffset.getTopicOffsets(offset)
    knownTopics = topicOffsets.keySet

    topicOffsets.toSeq.map {
      case (topic, start) => {
        PulsarContinuousTopic(
          topic, start, pulsarClientConf, pulsarReaderConf
        ): InputPartition[InternalRow]
      }
    }.asJava
  }

  override def mergeOffsets(partitionOffsets: Array[PartitionOffset]): Offset = {
    val mergedMap = partitionOffsets.map {
      case PulsarSourcePartitionOffset(t, o) => Map(t -> o)
    } reduce(_ ++ _)
    PulsarSourceOffset(mergedMap)
  }

  override def needsReconfiguration(): Boolean = {
    // TODO: support reconfiguration in future
    false
  }

  override def toString: String = s"PulsarSource[$offset]"

  override def commit(offset: Offset): Unit = {
    // NO-OP
  }

  override def stop(): Unit = {}

}

case class PulsarContinuousTopic(
    topic: String,
    messageId: MessageId,
    pulsarClientConf: ju.Map[String, Object],
    pulsarReaderConf: ju.Map[String, Object]) extends ContinuousInputPartition[InternalRow] {

  override def createContinuousReader(
      offset: PartitionOffset): InputPartitionReader[InternalRow] = {
    val pulsarOffset = offset.asInstanceOf[PulsarSourcePartitionOffset]
    require(pulsarOffset.topic == topic,
      s"Expected topic: $topic, but got: ${pulsarOffset.topic}")
    new PulsarContinuousTopicReader(
      topic,
      pulsarOffset.messageId,
      pulsarClientConf,
      pulsarReaderConf)
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new PulsarContinuousTopicReader(
      topic,
      messageId,
      pulsarClientConf,
      pulsarReaderConf
    )
  }
}

/**
 * A per task data reader for continuous pulsar processing.
 *
 * @param topic
 * @param startMessageId
 * @param pulsarConf
 */
class PulsarContinuousTopicReader(
    topic: String,
    startMessageId: MessageId,
    pulsarClientConf: ju.Map[String, Object],
    pulsarReaderConf: ju.Map[String, Object]) extends ContinuousInputPartitionReader[InternalRow] {
  private val converter = new PulsarMessageToUnsafeRowConverter
  private lazy val reader = CachedPulsarClient.getOrCreate(pulsarClientConf)
    .newReader(org.apache.pulsar.client.api.Schema.BYTES)
    .loadConf(pulsarReaderConf)
    .create()

  private var currentMessageId: MessageId = MessageId.earliest
  private var currentMessage: Message[Array[Byte]] = _

  override def getOffset: PartitionOffset = {
    PulsarSourcePartitionOffset(topic, currentMessageId)
  }

  override def next(): Boolean = {
    val m: Message[Array[Byte]] = reader.readNext()

    currentMessageId = m.getMessageId
    currentMessage = m
    true
  }

  override def get(): InternalRow = {
    converter.toUnsafeRow(currentMessage)
  }

  override def close(): Unit = {
    if (reader != null) {
      reader.close()
    }
  }
}
