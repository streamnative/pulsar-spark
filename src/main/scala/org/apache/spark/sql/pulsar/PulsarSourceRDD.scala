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
import java.util.concurrent.TimeUnit

import org.apache.pulsar.client.api.{Message, MessageId, PulsarClientException, Schema}
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}

import org.apache.spark.{Partition, SparkContext, SparkEnv, TaskContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.pulsar.PulsarSourceUtils._
import org.apache.spark.util.{LongAccumulator, NextIterator, Utils}

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
    jsonOptions: JSONOptionsInRead,
    pulsarClientFactoryClassName: Option[String])
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
      context: TaskContext,
      rowsBytesAccumulator: Option[LongAccumulator]): Iterator[InternalRow] = {

    val deserializer = new PulsarDeserializer(schemaInfo.si, jsonOptions)
    val schema: Schema[_] = SchemaUtils.getPSchema(schemaInfo.si)

    if (isTraceEnabled()) {
      logTrace(
        s" Start reading from topic ${topic}" +
        s" with subscription prefix ${subscriptionNamePrefix}" +
        s" from startOffset: ${startOffset} to endOffset: ${endOffset}"
      )
    }

    lazy val reader = PulsarClientFactory
      .getOrCreate(pulsarClientFactoryClassName, clientConf)
      .newReader(schema)
      .subscriptionRolePrefix(subscriptionNamePrefix)
      .topic(topic)
      .startMessageId(startOffset)
      .startMessageIdInclusive()
      .loadConf(readerConf)
      .create()

    val iter = new NextIterator[InternalRow] {

      private var inEnd: Boolean = false
      private var isLast: Boolean = false
      private val enterEndFunc: (MessageId => Boolean) = enteredEnd(endOffset)

      var currentMessage: Message[_] = _
      var currentId: MessageId = _

      try {
        if (!startOffset
            .isInstanceOf[UserProvidedMessageId] && startOffset != MessageId.earliest) {
          // Read and skip the first message when the start offset is exclusive.
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
            if (isTraceEnabled()) {
              logTrace(s"First message read has id ${currentId}")
            }

            (startOffset, currentId) match {
              case (_: BatchMessageIdImpl, _: BatchMessageIdImpl) =>
              // we seek using a batch message id, we can read next directly in `getNext()`
              case (_: MessageIdImpl, cbmid: BatchMessageIdImpl) =>
                // We can only really get to this scenario
                // when producers sent a batched message will a single record.
                // The reader will read a batched message but the message id
                // returned by getLastMessageId() will return a MessageIdImpl.
                assert(cbmid.getBatchIndex == 0,
                  s"Batch index should be 0, but got ${cbmid.getBatchIndex}")
              case (smid: MessageIdImpl, cmid: MessageIdImpl) =>
              // current entry is a non-batch entry, we can read next directly in `getNext()`
            }
          }
          // If start offset is exclusive and equal to end offset, don't read any data.
          if (currentId != null && enterEndFunc(currentId)) isLast = true
        }
      } catch {
        case e: PulsarClientException =>
          logError(s"PulsarClient failed to read message from topic $topic", e)
          close()
          throw e
        case e: Throwable =>
          throw e
      }

      private def processDataLoss(
          currentMessageId: MessageId,
          previousMessageId: MessageId): Unit = {

        reportDataLoss(
          s"Unexpected message skipping was detected:" +
            s" Previous message id was $previousMessageId," +
            s" however the current message read has id $currentMessageId"
        )
      }

      /**
       * Detect data loss by comparing the current message id with the previous message id.
       * Make sure invariants are held
       */
      private def detectDataLoss(
          currentMessageId: MessageId,
          previousMessageId: MessageId): Unit = {
        (currentMessageId, previousMessageId) match {
          case (c: BatchMessageIdImpl, p: BatchMessageIdImpl) =>
            if (c.getLedgerId == p.getLedgerId) {
              if (c.getEntryId == p.getEntryId) {
                if (c.getBatchIndex != p.getBatchIndex + 1) {
                  processDataLoss(c, p)
                }
              } else if (c.getEntryId == p.getEntryId + 1) {
                // if we have moved to ready the next entry
                // the batch index should be 0, i.e. to first record
                // in the entry / batch
                if (c.getBatchIndex != 0) {
                  processDataLoss(c, p)
                }
              } else if (c.getEntryId != p.getEntryId + 1) {
                processDataLoss(c, p)
              }
            }
          case (c: MessageIdImpl, p: BatchMessageIdImpl) =>
            if (c.getLedgerId == p.getLedgerId && c.getEntryId != p.getEntryId + 1) {
              processDataLoss(c, p)
            }

          case (c: MessageIdImpl, p: MessageIdImpl) =>
            // if we are still reading from the same ledger, the next message we read
            // should be the next entry in the ledger
            if (c.getLedgerId == p.getLedgerId && c.getEntryId != p.getEntryId + 1) {
              processDataLoss(c, p)
            }
        }
      }

      override protected def getNext(): InternalRow = {
        try {
          if (isLast) {
            finished = true
            return null
          }
          val prevMessage = currentMessage
          currentMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
          if (currentMessage == null) {
            reportDataLoss(
              s"We didn't get enough message as promised from topic $topic, data loss occurs")
            finished = true
            return null
          }

          // check for any data skipping
          val currentMessageId = currentMessage.getMessageId
          if (prevMessage != null) {
            val previousMessageId = prevMessage.getMessageId
            detectDataLoss(currentMessageId, previousMessageId)
          }

          rowsBytesAccumulator.foreach(_.add(currentMessage.size()))
          currentId = currentMessage.getMessageId

          finished = false
          inEnd = enterEndFunc(currentId)
          if (inEnd) {
            isLast = isLastMessage(currentId)
          }

          if (isTraceEnabled()) {
            logTrace(
              s"Read message:" +
              s" currentId=${currentId}" +
              s" isLast:$isLast, inEnd:$inEnd"
            )
          }
          deserializer.deserialize(currentMessage)
        } catch {
          case e: PulsarClientException =>
            logError(s"PulsarClient failed to read message from topic $topic", e)
            throw e
          case e: Throwable =>
            throw e
        }
      }

      override protected def close(): Unit = {
        reader.close()
      }
    }
    context.addTaskCompletionListener[Unit] { _ =>
      iter.closeIfNeeded()
    }
    iter
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
    jsonOptions: JSONOptionsInRead,
    rowsBytesAccumulator: LongAccumulator,
    pulsarClientFactoryClassName: Option[String])
    extends PulsarSourceRDDBase(
      sc,
      schemaInfo,
      clientConf,
      readerConf,
      offsetRanges,
      pollTimeoutMs,
      failOnDataLoss,
      subscriptionNamePrefix,
      jsonOptions,
      pulsarClientFactoryClassName) {

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

    computeInner(tp, start, end, context, Some(rowsBytesAccumulator))
  }
}

private[pulsar] class PulsarSourceRDD4Batch(
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
      jsonOptions,
      None) {

  override def compute(split: Partition, context: TaskContext): Iterator[InternalRow] = {

    val part = split.asInstanceOf[PulsarSourceRDDPartition]
    val tp = part.offsetRange.topic
    val start = part.offsetRange.fromOffset
    val end = part.offsetRange.untilOffset

    if (start == end || !messageExists(end)) {
      return Iterator.empty
    }

    computeInner(tp, start, end, context, None)
  }
}
