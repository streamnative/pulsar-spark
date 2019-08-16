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
import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.util.UUID

import org.apache.pulsar.client.api.{Message, MessageId, Schema, SubscriptionType}
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}
import org.apache.pulsar.common.schema.SchemaInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
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
    initialOffset: PerTopicOffset,
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends ContinuousReader
    with Logging {

  // Initialized when creating reader factories. If this diverges from the partitions at the latest
  // offsets, we need to reconfigure.
  // Exposed outside this object only for unit tests.
  @volatile private[sql] var knownTopics: Set[String] = _

  lazy val pulsarSchema: SchemaInfo = metadataReader.getPulsarSchema()

  override def readSchema(): StructType = SchemaUtils.pulsarSourceSchema(pulsarSchema)

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  private var offset: Offset = _
  override def setStartOffset(start: ju.Optional[Offset]): Unit = {
    offset = start.orElse {
      val actualOffsets = SpecificPulsarOffset(
        metadataReader.actualOffsets(initialOffset, 120 * 1000, reportDataLoss))
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

    val oldStartPartitionOffsets = SpecificPulsarOffset.getTopicOffsets(offset)
    val currentPartitionSet = metadataReader.fetchLatestOffsets().topicOffsets.keySet
    val newPartitions = currentPartitionSet.diff(oldStartPartitionOffsets.keySet)
    val newPartitionOffsets = metadataReader.fetchEarliestOffsets(newPartitions.toSeq)
    val deletedPartitions = oldStartPartitionOffsets.keySet.diff(currentPartitionSet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"Some topics were deleted: $deletedPartitions")
    }

    val startOffsets = newPartitionOffsets ++
      oldStartPartitionOffsets.filterKeys(!deletedPartitions.contains(_))
    knownTopics = startOffsets.keySet

    startOffsets.toSeq.map {
      case (topic, start) =>
        new PulsarContinuousTopic(
          topic,
          new SchemaInfoSerializable(pulsarSchema),
          start,
          clientConf,
          consumerConf,
          pollTimeoutMs,
          failOnDataLoss,
          subscriptionNamePrefix,
          jsonOptions
        ): InputPartition[InternalRow]
    }.asJava
  }

  override def mergeOffsets(partitionOffsets: Array[PartitionOffset]): Offset = {
    val mergedMap = partitionOffsets
      .map {
        case PulsarPartitionOffset(t, o) => Map(t -> o)
      }
      .reduce(_ ++ _)
    SpecificPulsarOffset(mergedMap)
  }

  override def needsReconfiguration(): Boolean = {
    knownTopics != null && metadataReader.fetchLatestOffsets().topicOffsets.keySet != knownTopics
  }

  override def toString: String = s"PulsarSource[$offset]"

  override def commit(offset: Offset): Unit = {
    val off = SpecificPulsarOffset.getTopicOffsets(offset)
    metadataReader.commitCursorToOffset(off)
  }

  override def stop(): Unit = {
    metadataReader.removeCursor()
    metadataReader.close()
  }
}

private[pulsar] class PulsarContinuousTopic(
    var topic: String,
    var schemaInfo: SchemaInfoSerializable,
    var startingOffsets: MessageId,
    var clientConf: ju.Map[String, Object],
    var consumerConf: ju.Map[String, Object],
    var pollTimeoutMs: Int,
    var failOnDataLoss: Boolean,
    var subscriptionNamePrefix: String,
    var jsonOptions: JSONOptionsInRead)
    extends ContinuousInputPartition[InternalRow]
    with Externalizable {

  def this() =
    this(null, null, null, null, null, 0, false, null, null) // For deserialization only

  override def createContinuousReader(
      offset: PartitionOffset): InputPartitionReader[InternalRow] = {
    val pulsarOffset = offset.asInstanceOf[PulsarPartitionOffset]
    require(
      pulsarOffset.topic == topic,
      s"Expected topic: $topic, but got: ${pulsarOffset.topic}")
    new PulsarContinuousTopicReader(
      topic,
      schemaInfo,
      pulsarOffset.messageId,
      clientConf,
      consumerConf,
      pollTimeoutMs,
      failOnDataLoss,
      subscriptionNamePrefix,
      jsonOptions)
  }

  override def createPartitionReader(): InputPartitionReader[InternalRow] = {
    new PulsarContinuousTopicReader(
      topic,
      schemaInfo,
      startingOffsets,
      clientConf,
      consumerConf,
      pollTimeoutMs,
      failOnDataLoss,
      subscriptionNamePrefix,
      jsonOptions)
  }

  override def writeExternal(out: ObjectOutput): Unit = {
    out.writeUTF(topic)
    out.writeObject(schemaInfo)
    out.writeObject(clientConf)
    out.writeObject(consumerConf)
    out.writeInt(pollTimeoutMs)
    out.writeBoolean(failOnDataLoss)
    out.writeUTF(subscriptionNamePrefix)

    val bytes = startingOffsets.toByteArray
    out.writeInt(bytes.length)
    out.write(bytes)
    out.writeObject(jsonOptions)
  }

  override def readExternal(in: ObjectInput): Unit = {
    topic = in.readUTF()
    schemaInfo = in.readObject().asInstanceOf[SchemaInfoSerializable]
    clientConf = in.readObject().asInstanceOf[ju.Map[String, Object]]
    consumerConf = in.readObject().asInstanceOf[ju.Map[String, Object]]
    failOnDataLoss = in.readBoolean()
    pollTimeoutMs = in.readInt()
    subscriptionNamePrefix = in.readUTF()

    val length = in.readInt()
    val bytes = new Array[Byte](length)
    in.read(bytes)

    startingOffsets = MessageId.fromByteArray(bytes)
    jsonOptions = in.readObject().asInstanceOf[JSONOptionsInRead]
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
    schemaInfo: SchemaInfoSerializable,
    startingOffsets: MessageId,
    clientConf: ju.Map[String, Object],
    consumerConf: ju.Map[String, Object],
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends ContinuousInputPartitionReader[InternalRow] {

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  private val deserializer = new PulsarDeserializer(schemaInfo.si, jsonOptions)
  private val schema: Schema[_] = SchemaUtils.getPSchema(schemaInfo.si)
  private val consumer = CachedPulsarClient
    .getOrCreate(clientConf)
    .newConsumer(schema)
    .topic(topic)
    .subscriptionName(s"$subscriptionNamePrefix-${UUID.randomUUID()}")
    .subscriptionType(SubscriptionType.Exclusive)
    .loadConf(consumerConf)
    .subscribe()

  try {
    consumer.seek(startingOffsets)
  } catch {
    case e: Throwable =>
      reportDataLoss(s"Failed to seek to previous $startingOffsets, data loss occurs")
  }

  var currentMessage: Message[_] = _
  var currentId: MessageId = _

  if (!startingOffsets.isInstanceOf[UserProvidedMessageId]
      && startingOffsets != MessageId.earliest) {
    currentMessage = consumer.receive()
    currentId = currentMessage.getMessageId
    if (startingOffsets != MessageId.earliest && !messageIdRoughEquals(
          currentId,
          startingOffsets)) {
      reportDataLoss(
        s"Potential Data Loss: intended to start at $startingOffsets, " +
          s"actually we get $currentId")
    }

    (startingOffsets, currentId) match {
      case (_: BatchMessageIdImpl, _: BatchMessageIdImpl) =>
      // we seek using a batch message id, we can read next directly in `getNext()`
      case (_: MessageIdImpl, cbmid: BatchMessageIdImpl) =>
        // we seek using a message id, this is supposed to be read by previous task since it's
        // inclusive for the last batch (start, end], so we skip this batch
        val newStart =
          new MessageIdImpl(cbmid.getLedgerId, cbmid.getEntryId + 1, cbmid.getPartitionIndex)
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
    deserializer.deserialize(currentMessage)
  }

  override def close(): Unit = {
    if (consumer != null) {
      consumer.unsubscribe()
      consumer.close()
    }
  }
}
