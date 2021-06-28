/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.pulsar

import java.{util => ju}
import java.util.concurrent.TimeUnit

import org.apache.pulsar.client.api.{Message, MessageId, Schema}
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}
import org.apache.spark.sql.pulsar.PulsarSourceUtils._
import org.apache.spark.util.Utils

private case class PulsarBatchInputPartition(
                                              offsetRange: PulsarOffsetRange,
                                              pSchemaSerializable: SchemaInfoSerializable,
                                              adminUrl: String,
                                              clientConf: ju.Map[String, Object],
                                              readerConf: ju.Map[String, Object],
                                              pollTimeoutMs: Int,
                                              failOnDataLoss: Boolean,
                                              subscriptionNamePrefix: String,
                                              jsonOptions: JSONOptionsInRead
                                            ) extends InputPartition

private object PulsarBatchReaderFactory extends PartitionReaderFactory {

  override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
    val p = partition.asInstanceOf[PulsarBatchInputPartition]

    val start = p.offsetRange.fromOffset
    val end = p.offsetRange.untilOffset match {
      case MessageId.latest =>
        Utils.tryWithResource(AdminUtils.buildAdmin(p.adminUrl, p.clientConf)) { admin =>
          PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(p.offsetRange.topic))
        }
      case id => id
    }

    val updatedOffsetRange = PulsarOffsetRange(p.offsetRange.topic, start, end, p.offsetRange.preferredLoc)

    if (start == end || !messageExists(end)) {
      return PulsarMicroBatchEmptyInputPartitionReader
    }

    PulsarBatchPartitionReader(
      updatedOffsetRange,
      p.pSchemaSerializable,
      p.clientConf,
      p.readerConf,
      p.pollTimeoutMs,
      p.failOnDataLoss,
      p.jsonOptions)
  }
}

object PulsarMicroBatchEmptyInputPartitionReader
  extends PartitionReader[InternalRow]
    with Logging {
  override def next(): Boolean = false
  override def get(): InternalRow = null
  override def close(): Unit = {}
}

private case class PulsarBatchPartitionReader(
                                               offsetRange: PulsarOffsetRange,
                                               pSchemaSerializable: SchemaInfoSerializable,
                                               clientConf: ju.Map[String, Object],
                                               readerConf: ju.Map[String, Object],
                                               pollTimeoutMs: Int,
                                               failOnDataLoss: Boolean,
                                               jsonOptions: JSONOptionsInRead
                                             ) extends PartitionReader[InternalRow] with Logging {

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  val deserializer = new PulsarDeserializer(pSchemaSerializable.si, jsonOptions)
  val schema: Schema[_] = SchemaUtils.getPSchema(pSchemaSerializable.si)

  val (topic, startOffset, endOffset) = (
    offsetRange.topic,
    offsetRange.fromOffset,
    offsetRange.untilOffset)

  val reader = CachedPulsarClient
    .getOrCreate(clientConf)
    .newReader(schema)
    .topic(topic)
    .startMessageId(startOffset)
    .startMessageIdInclusive()
    .loadConf(readerConf)
    .create()

  private var inEnd: Boolean = false
  private var isLast: Boolean = false
  private val enterEndFunc: MessageId => Boolean = enteredEnd(endOffset)

  private var nextRow: InternalRow = _
  private var nextMessage: Message[_] = _
  private var nextId: MessageId = _

  if (!startOffset.isInstanceOf[UserProvidedMessageId] && startOffset != MessageId.earliest) {
    nextMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
    if (nextMessage == null) {
      isLast = true
      reportDataLoss(s"cannot read data at $startOffset from topic $topic")
    } else {
      nextId = nextMessage.getMessageId

      if (startOffset != MessageId.earliest && !messageIdRoughEquals(nextId, startOffset)) {
        reportDataLoss(
          s"Potential Data Loss in reading topic $topic: intended to start at $startOffset, " +
            s"actually we get $nextId")
      }

      (startOffset, nextId) match {
        case (_: BatchMessageIdImpl, _: BatchMessageIdImpl) =>
        // we seek using a batch message id, we can read next directly in `get()`
        case (_: MessageIdImpl, cbmid: BatchMessageIdImpl) =>
          // we seek using a message id, this is supposed to be read by previous task since it's
          // inclusive for the last batch (start, end], so we skip this batch
          val newStart = new MessageIdImpl(
            cbmid.getLedgerId,
            cbmid.getEntryId + 1,
            cbmid.getPartitionIndex)
          reader.seek(newStart)
        case (smid: MessageIdImpl, cmid: MessageIdImpl) =>
        // current entry is a non-batch entry, we can read next directly in `get()`
      }
    }
  } else {
    nextId = startOffset
  }

  override def next(): Boolean = {

    if (isLast) {
      return false
    }

    nextMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)

    if (nextMessage == null) {
      reportDataLoss(
        s"We didn't get enough message as promised from topic $topic, data loss occurs")
      return false
    }

    nextId = nextMessage.getMessageId

    nextRow = deserializer.deserialize(nextMessage)

    inEnd = enterEndFunc(nextId)
    if (inEnd) {
      isLast = isLastMessage(nextId)
    }

    true
  }

  override def get(): InternalRow = {
    assert(nextRow != null)
    nextRow
  }

  override def close(): Unit =
    reader.close()

}

