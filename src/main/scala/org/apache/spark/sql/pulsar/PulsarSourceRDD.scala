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
import java.util.UUID
import java.util.concurrent.TimeUnit

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{Message, MessageId, Schema, SubscriptionType}
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}

import org.apache.spark.{Partition, SparkContext, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.pulsar.PulsarSourceUtils._
import org.apache.spark.util.{NextIterator, Utils}

private[pulsar] case class PulsarSourceRDDPartition(index: Int, offsetRange: PulsarOffsetRange)
    extends Partition

private[pulsar] abstract class PulsarSourceRDDBase(
    sc: SparkContext,
    schemaInfo: SchemaInfoSerializable,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    offsetRanges: Seq[PulsarOffsetRange],
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends RDD[InternalRow](sc, Nil) {

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  override protected def getPartitions: Array[Partition] = {
    offsetRanges.zipWithIndex.map { case (o, i) =>
      new PulsarSourceRDDPartition(i, o)
    }.toArray
  }

  def computeInner(
      topic: String,
      startOffset: MessageId,
      endOffset: MessageId,
      context: TaskContext): Iterator[InternalRow] = {

    val deserializer = new PulsarDeserializer(schemaInfo.si, jsonOptions)
    val schema: Schema[_] = SchemaUtils.getPSchema(schemaInfo.si)

    lazy val reader = CachedPulsarClient
      .getOrCreate(clientConf)
      .newReader(schema)
      .subscriptionRolePrefix(subscriptionNamePrefix)
      .topic(topic)
      .startMessageId(startOffset)
      .startMessageIdInclusive()
      .loadConf(readerConf)
      .create()

    new NextIterator[InternalRow] {

      private var inEnd: Boolean = false
      private var isLast: Boolean = false
      private val enterEndFunc: (MessageId => Boolean) = enteredEnd(endOffset)

      var currentMessage: Message[_] = _
      var currentId: MessageId = _

      if (!startOffset.isInstanceOf[UserProvidedMessageId] && startOffset != MessageId.earliest) {
        currentMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
        if (currentMessage == null) {
          isLast = true
          reportDataLoss(s"cannot read data at $startOffset from topic $topic")
        } else {
          currentId = currentMessage.getMessageId
          if (startOffset != MessageId.earliest && !messageIdRoughEquals(
              currentId,
              startOffset)) {
            reportDataLoss(
              s"Potential Data Loss: intended to start at $startOffset, " +
                s"actually we get $currentId")
          }

          (startOffset, currentId) match {
            case (_: BatchMessageIdImpl, _: BatchMessageIdImpl) =>
            // we seek using a batch message id, we can read next directly in `getNext()`
            case (_: MessageIdImpl, cbmid: BatchMessageIdImpl) =>
              // we seek using a message id, this is supposed to be read by previous task since it's
              // inclusive for the last batch (start, end], so we skip this batch
              val newStart = new MessageIdImpl(
                cbmid.getLedgerId,
                cbmid.getEntryId + 1,
                cbmid.getPartitionIndex)
              reader.seek(newStart)
            case (smid: MessageIdImpl, cmid: MessageIdImpl) =>
            // current entry is a non-batch entry, we can read next directly in `getNext()`
          }
        }
      }

      override protected def getNext(): InternalRow = {
        if (isLast) {
          finished = true
          return null
        }
        currentMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
        if (currentMessage == null) {
          reportDataLoss(
            s"We didn't get enough message as promised from topic $topic, data loss occurs")
          finished = true
          return null
        }

        currentId = currentMessage.getMessageId

        finished = false
        inEnd = enterEndFunc(currentId)
        if (inEnd) {
          isLast = isLastMessage(currentId)
        }
        deserializer.deserialize(currentMessage)
      }

      override protected def close(): Unit = {
        reader.close()
      }
    }
  }
}

private[pulsar] class PulsarSourceRDD(
    sc: SparkContext,
    schemaInfo: SchemaInfoSerializable,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    offsetRanges: Seq[PulsarOffsetRange],
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends PulsarSourceRDDBase(
      sc,
      schemaInfo,
      clientConf,
      readerConf,
      offsetRanges,
      pollTimeoutMs,
      failOnDataLoss,
      subscriptionNamePrefix,
      jsonOptions) {

  override def getPreferredLocations(split: Partition): Seq[String] = {
    val part = split.asInstanceOf[PulsarSourceRDDPartition]
    part.offsetRange.preferredLoc.map(Seq(_)).getOrElse(Seq.empty)
  }

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val part = split.asInstanceOf[PulsarSourceRDDPartition]
    val tp = part.offsetRange.topic
    val start = part.offsetRange.fromOffset
    val end = part.offsetRange.untilOffset

    if (start == end || !messageExists(end)) {
      return Iterator.empty
    }

    computeInner(tp, start, end, context)
  }
}

private[pulsar] class PulsarSourceRDD4Batch(
    sc: SparkContext,
    schemaInfo: SchemaInfoSerializable,
    adminUrl: String,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    offsetRanges: Seq[PulsarOffsetRange],
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends PulsarSourceRDDBase(
      sc,
      schemaInfo,
      clientConf,
      readerConf,
      offsetRanges,
      pollTimeoutMs,
      failOnDataLoss,
      subscriptionNamePrefix,
      jsonOptions) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val part = split.asInstanceOf[PulsarSourceRDDPartition]
    val tp = part.offsetRange.topic
    val start = part.offsetRange.fromOffset
    val end = part.offsetRange.untilOffset match {
      case MessageId.latest =>
        Utils.tryWithResource(AdminUtils.buildAdmin(adminUrl, clientConf)) { admin =>
          PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp))
        }
      case id => id
    }

    if (start == end || !messageExists(end)) {
      return Iterator.empty
    }

    computeInner(tp, start, end, context)
  }
}
