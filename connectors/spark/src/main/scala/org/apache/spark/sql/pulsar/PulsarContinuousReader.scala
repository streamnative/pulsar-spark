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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.UUID
import java.{util => ju}

import org.apache.pulsar.client.api.{Message, MessageId, SubscriptionType}
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.pulsar.PulsarSourceUtils.{messageIdRoughEquals, reportDataLossFunc}
import org.apache.spark.sql.sources.v2.reader.{ContinuousInputPartition, InputPartition, InputPartitionReader}
import org.apache.spark.sql.sources.v2.reader.streaming.{ContinuousInputPartitionReader, ContinuousReader, Offset, PartitionOffset}
import org.apache.spark.sql.types.StructType

/**
 * A [[ContinuousReader]] for reading data from Pulsar.
  *
 * @param clientConf
 * @param consumerConf
 * @param initialOffset
 */
class PulsarContinuousReader(
    metadataReader: PulsarMetadataReader,
    clientConf: ju.Map[String, Object],
    consumerConf: ju.Map[String, Object],
    initialOffset: SpecificPulsarOffset,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String) extends ContinuousReader with Logging {

  // Initialized when creating reader factories. If this diverges from the partitions at the latest
  // offsets, we need to reconfigure.
  // Exposed outside this object only for unit tests.
  @volatile private[sql] var knownTopics: Set[String] = _

  override def readSchema(): StructType = PulsarReader.pulsarSchema

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  private var offset: Offset = _
  override def setStartOffset(start: ju.Optional[Offset]): Unit = {
    offset = start.orElse {
      val actualOffsets = SpecificPulsarOffset(
        metadataReader.fetchCurrentOffsets(initialOffset, reportDataLoss))
      logInfo(s"Initial Offsets: $actualOffsets")
      actualOffsets
    }
  }
  override def getStartOffset: Offset = offset

  override def deserializeOffset(json: String): Offset = {
    SpecificPulsarOffset(JsonUtils.topicOffsets(json))
  }

  override def planInputPartitions(): ju.List[InputPartition[InternalRow]] = {
    import scala.collection.JavaConverters._

    val topicOffsets = SpecificPulsarOffset.getTopicOffsets(offset)
    knownTopics = topicOffsets.keySet

    topicOffsets.toSeq.map {
      case (topic, start) => {
        new PulsarContinuousTopic(
          topic, start, clientConf, consumerConf,
          failOnDataLoss, subscriptionNamePrefix
        ): InputPartition[InternalRow]
      }
    }.asJava
  }

  override def mergeOffsets(partitionOffsets: Array[PartitionOffset]): Offset = {
    val mergedMap = partitionOffsets.map {
      case PulsarPartitionOffset(t, o) => Map(t -> o)
    }.reduce(_ ++ _)
    SpecificPulsarOffset(mergedMap)
  }

  override def needsReconfiguration(): Boolean = {
    // TODO: support reconfiguration in future
    false
  }

  override def toString: String = s"PulsarSource[$offset]"

  override def commit(offset: Offset): Unit = {
    val off = SpecificPulsarOffset.getTopicOffsets(offset)
    metadataReader.commitCursorToOffset(off)
  }

  override def stop(): Unit = {
    metadataReader.removeCursor()
    metadataReader.stop()
  }
}

private[pulsar] class PulsarContinuousTopic(
    var topic: String,
    var startingOffsets: MessageId,
    var clientConf: ju.Map[String, Object],
    var consumerConf: ju.Map[String, Object],
    var failOnDataLoss: Boolean,
    var subscriptionNamePrefix: String) extends ContinuousInputPartition[InternalRow] with Externalizable {

  def this() = this(null, null, null, null, false, null) // For deserialization only

  override def createContinuousReader(
      offset: PartitionOffset): InputPartitionReader[InternalRow] = {
    val pulsarOffset = offset.asInstanceOf[PulsarPartitionOffset]
    require(pulsarOffset.topic == topic,
      s"Expected topic: $topic, but got: ${pulsarOffset.topic}")
    new PulsarContinuousTopicReader(
      topic,
      pulsarOffset.messageId,
      clientConf,
      consumerConf,
      failOnDataLoss,
      subscriptionNamePrefix)
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new PulsarContinuousTopicReader(
      topic,
      startingOffsets,
      clientConf,
      consumerConf,
      failOnDataLoss,
      subscriptionNamePrefix)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(topic)
    out.writeObject(clientConf)
    out.writeObject(consumerConf)
    out.writeBoolean(failOnDataLoss)
    out.writeUTF(subscriptionNamePrefix)

    val bytes = startingOffsets.toByteArray
    out.writeInt(bytes.length)
    out.write(bytes)
  }

  override def readExternal(in: ObjectInput): Unit = {
    topic = in.readUTF()
    clientConf = in.readObject().asInstanceOf[ju.Map[String, Object]]
    consumerConf = in.readObject().asInstanceOf[ju.Map[String, Object]]
    failOnDataLoss = in.readBoolean()
    subscriptionNamePrefix = in.readUTF()

    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.read(bytes)

    startingOffsets = MessageId.fromByteArray(bytes)
  }
}

/**
 * A per task data reader for continuous pulsar processing.
 *
 * @param topic
 * @param startingOffsets
 * @param clientConf
 * @param consumerConf
 */
class PulsarContinuousTopicReader(
    topic: String,
    startingOffsets: MessageId,
    clientConf: ju.Map[String, Object],
    consumerConf: ju.Map[String, Object],
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String) extends ContinuousInputPartitionReader[InternalRow] {
  private val converter = new PulsarMessageToUnsafeRowConverter
  private val consumer = CachedPulsarClient.getOrCreate(clientConf)
    .newConsumer()
    .topic(topic)
    .subscriptionName(s"$subscriptionNamePrefix-${UUID.randomUUID()}")
    .subscriptionType(SubscriptionType.Exclusive)
    .loadConf(consumerConf)
    .subscribe()
  consumer.seek(startingOffsets)

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  var currentMessage: Message[Array[Byte]] = _
  var currentId: MessageId = _

  if (startingOffsets != MessageId.earliest) {
    currentMessage = consumer.receive()
    currentId = currentMessage.getMessageId
    if (startingOffsets != MessageId.earliest && !messageIdRoughEquals(currentId, startingOffsets)) {
      reportDataLoss(s"Potential Data Loss: intended to start at $startingOffsets, " +
        s"actually we get $currentId")
    }

    (startingOffsets, currentId) match {
      case (_: BatchMessageIdImpl, _: BatchMessageIdImpl) =>
      // we seek using a batch message id, we can read next directly in `getNext()`
      case (_: MessageIdImpl, cbmid: BatchMessageIdImpl) =>
        // we seek using a message id, this is supposed to be read by previous task since it's
        // inclusive for the last batch (start, end], so we skip this batch
        val newStart = new MessageIdImpl(cbmid.getLedgerId, cbmid.getEntryId + 1, cbmid.getPartitionIndex)
        consumer.seek(newStart)
      case (smid: MessageIdImpl, cmid: MessageIdImpl) =>
      // current entry is a non-batch entry, we can read next directly in `getNext()`
    }
  } else {
    currentId = MessageId.earliest
  }

  // use general `MessageIdImpl` while talking with Spark,
  // and internally deal with batchMessageIdImpl and MessageIdImpl
  override def getOffset: PartitionOffset = {
    PulsarPartitionOffset(topic, PulsarSourceUtils.mid2Impl(currentId))
  }

  override def next(): Boolean = {
    currentMessage = consumer.receive()
    currentId = currentMessage.getMessageId
    true
  }

  override def get(): InternalRow = {
    converter.toUnsafeRow(currentMessage)
  }

  override def close(): Unit = {
    if (consumer != null) {
      consumer.unsubscribe()
      consumer.close()
    }
  }
}
