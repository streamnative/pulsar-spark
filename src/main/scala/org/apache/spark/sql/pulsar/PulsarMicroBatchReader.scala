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

import org.apache.pulsar.client.api.{Message, MessageId, Schema}
import org.apache.pulsar.client.impl.{BatchMessageIdImpl, MessageIdImpl}
import org.apache.pulsar.common.schema.SchemaInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.read.{
  InputPartition,
  PartitionReader,
  PartitionReaderFactory
}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}
import org.apache.spark.sql.execution.streaming.{Offset => eOffset}

private[pulsar] class PulsarMicroBatchReader(
    metadataReader: PulsarMetadataReader,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    metadataPath: String,
    startingOffsets: PerTopicOffset,
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends MicroBatchStream
    with Logging {

  import PulsarSourceUtils._

  private var startTopicOffsets: Map[String, MessageId] = _
  private var endTopicOffsets: Map[String, MessageId] = _
  private var stopped = false

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  private lazy val initialTopicOffsets: SpecificPulsarOffset = {
    val metadataLog =
      new PulsarSourceInitialOffsetWriter(SparkSession.getActiveSession.get, metadataPath)
    metadataLog.getInitialOffset(metadataReader, startingOffsets, pollTimeoutMs, reportDataLoss)
  }

  override def deserializeOffset(json: String): Offset = {
    SpecificPulsarOffset(JsonUtils.topicOffsets(json))
  }

  lazy val pulsarSchema: SchemaInfo = metadataReader.getPulsarSchema()

  override def planInputPartitions(start: Offset, end: Offset): Array[InputPartition] = {
    val newPartitions = endTopicOffsets.keySet.diff(startTopicOffsets.keySet)
    val newPartitionInitialOffsets = metadataReader.fetchEarliestOffsets(newPartitions.toSeq)
    logInfo(s"Topics added: $newPartitions")

    val deletedPartitions = startTopicOffsets.keySet.diff(endTopicOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"$deletedPartitions are gone. Some data may have been missed")
    }

    val newStartsOffsets = startTopicOffsets ++ newPartitionInitialOffsets

    val offsetRanges = endTopicOffsets.keySet
      .map { tp =>
        val fromOffset = newStartsOffsets.getOrElse(
          tp, {
            // this shouldn't happen
            throw new IllegalStateException(s"$tp doesn't have a start offset")
          })
        val untilOffset = endTopicOffsets(tp)
        val sortedExecutors = getSortedExecutorList()
        val numExecutors = sortedExecutors.length
        val preferredLoc = if (numExecutors > 0) {
          // This allows cached PulsarClient in the executors to be re-used to read the same
          // partition in every batch.
          Some(sortedExecutors(Math.floorMod(tp.hashCode, numExecutors)))
        } else None
        PulsarOffsetRange(tp, fromOffset, untilOffset, preferredLoc)
      }
      .filter { range =>
        if (range.untilOffset.compareTo(range.fromOffset) < 0 &&
          range.fromOffset.asInstanceOf[MessageIdImpl] != MessageId.latest) {
          reportDataLoss(
            s"${range.topic}'s offset was changed " +
              s"from ${range.fromOffset} to ${range.untilOffset}, " +
              "some data might has been missed")
          false
        } else if (range.untilOffset.compareTo(range.fromOffset) < 0 &&
          range.fromOffset.asInstanceOf[MessageIdImpl] == MessageId.latest) {
          false
        } else {
          true
        }
      }
      .toSeq

    offsetRanges.map { range =>
      new PulsarMicroBatchInputPartition(
        range,
        new SchemaInfoSerializable(pulsarSchema),
        clientConf,
        readerConf,
        pollTimeoutMs,
        failOnDataLoss,
        subscriptionNamePrefix,
        jsonOptions).asInstanceOf[InputPartition]
    }.toArray
  }

  override def stop(): Unit = synchronized {
    if (!stopped) {
      metadataReader.removeCursor()
      metadataReader.close()
      stopped = true
    }
  }

  override def createReaderFactory(): PartitionReaderFactory = new PartitionReaderFactory {
    override def createReader(partition: InputPartition): PartitionReader[InternalRow] = {
      val range = partition.asInstanceOf[PulsarMicroBatchInputPartition].range
      val start = range.fromOffset
      val end = range.untilOffset

      if (start == end || !messageExists(end)) {
        return PulsarMicroBatchEmptyInputPartitionReader
      }
      new PulsarMicroBatchInputPartitionReader(
        range,
        new SchemaInfoSerializable(pulsarSchema),
        clientConf,
        readerConf,
        pollTimeoutMs,
        failOnDataLoss,
        subscriptionNamePrefix,
        jsonOptions)
    }
  }

  override def initialOffset(): Offset = SpecificPulsarOffset(startTopicOffsets)

  override def latestOffset(): Offset = SpecificPulsarOffset(endTopicOffsets)

  override def commit(end: Offset): Unit = {
    val endTopicOffsets = SpecificPulsarOffset.getTopicOffsets(end.asInstanceOf[eOffset])
    metadataReader.commitCursorToOffset(endTopicOffsets)
  }
}

case class PulsarMicroBatchInputPartition(
    range: PulsarOffsetRange,
    pulsarSchema: SchemaInfoSerializable,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends InputPartition {
  override def preferredLocations(): Array[String] = range.preferredLoc.toArray
}

object PulsarMicroBatchEmptyInputPartitionReader
    extends PartitionReader[InternalRow]
    with Logging {

  override def next(): Boolean = false
  override def get(): InternalRow = null
  override def close(): Unit = {}
}

case class PulsarMicroBatchInputPartitionReader(
    range: PulsarOffsetRange,
    pulsarSchema: SchemaInfoSerializable,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends PartitionReader[InternalRow]
    with Logging {

  import PulsarSourceUtils._

  val tp = range.topic
  val start = range.fromOffset
  val end = range.untilOffset

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  private val deserializer = new PulsarDeserializer(pulsarSchema.si, jsonOptions)
  private val schema: Schema[_] = SchemaUtils.getPSchema(pulsarSchema.si)
  val reader = CachedPulsarClient
    .getOrCreate(clientConf)
    .newReader(schema)
    .subscriptionRolePrefix(subscriptionNamePrefix)
    .startMessageId(start)
    .startMessageIdInclusive()
    .topic(tp)
    .loadConf(readerConf)
    .create()

  private var inEnd: Boolean = false
  private var isLast: Boolean = false
  private val enterEndFunc: (MessageId => Boolean) = enteredEnd(end)

  private var nextRow: InternalRow = _
  private var nextMessage: Message[_] = _
  private var nextId: MessageId = _

  if (!start.isInstanceOf[UserProvidedMessageId] && start != MessageId.earliest) {
    nextMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
    if (nextMessage == null) {
      isLast = true
      reportDataLoss(s"Cannot read data at offset $start from topic: $tp")
    } else {
      nextId = nextMessage.getMessageId
      if (start != MessageId.earliest && !messageIdRoughEquals(nextId, start)) {
        reportDataLoss(
          s"Potential Data Loss in reading $tp: intended to start at $start, " +
            s"actually we get $nextId")
      }

      (start, nextId) match {
        case (_: BatchMessageIdImpl, _: BatchMessageIdImpl) =>
        // we seek using a batch message id, we can read next directly in `getNext()`
        case (_: MessageIdImpl, cbmid: BatchMessageIdImpl) =>
          // we seek using a message id, this is supposed to be read by previous task since it's
          // inclusive for the last batch (start, end], so we skip this batch
          val newStart =
            new MessageIdImpl(cbmid.getLedgerId, cbmid.getEntryId + 1, cbmid.getPartitionIndex)
          reader.seek(newStart)
        case (smid: MessageIdImpl, cmid: MessageIdImpl) =>
        // current entry is a non-batch entry, we can read next directly in `getNext()`
      }
    }
  } else {
    nextId = start
  }

  override def next(): Boolean = {
    if (isLast) {
      return false
    }

    nextMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)

    if (nextMessage == null) {
      // Losing some data. Skip the rest offsets in this partition.
      reportDataLoss(
        s"we didn't get enough messages as promised from topic $tp, data loss occurs")
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

  override def close(): Unit = {
    reader.close()
  }
}
