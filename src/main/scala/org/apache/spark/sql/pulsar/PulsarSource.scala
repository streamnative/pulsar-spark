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

import org.apache.pulsar.client.admin.PulsarAdmin

import java.{util => ju}
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.pulsar.client.internal.DefaultImplementation
import org.apache.pulsar.common.schema.SchemaInfo
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit, ReadMaxFiles, SupportsAdmissionControl}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.pulsar.PulsarOptions.ServiceUrlOptionKey
import org.apache.spark.sql.pulsar.SpecificPulsarOffset.getTopicOffsets
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

private[pulsar] class PulsarSource(
    serviceUrl: String,
    sqlContext: SQLContext,
    pulsarHelper: PulsarHelper,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    metadataPath: String,
    startingOffsets: PerTopicOffset,
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends Source
    with Logging
    with SupportsAdmissionControl {

  import PulsarSourceUtils._

  private val sc = sqlContext.sparkContext

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)
  private var stopped = false

  private lazy val initialTopicOffsets: SpecificPulsarOffset = {
    val metadataLog = new PulsarSourceInitialOffsetWriter(sqlContext.sparkSession, metadataPath)
    metadataLog.getInitialOffset(pulsarHelper, startingOffsets, pollTimeoutMs, reportDataLoss)
  }

  private var currentTopicOffsets: Option[Map[String, MessageId]] = None

  private lazy val pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(serviceUrl).build()

  private lazy val pulsarSchema: SchemaInfo = pulsarHelper.getPulsarSchema

  override def schema(): StructType = SchemaUtils.pulsarSourceSchema(pulsarSchema)

  override def getOffset: Option[Offset] = {
    // Make sure initialTopicOffsets is initialized
    initialTopicOffsets
    val latest = pulsarHelper.fetchLatestOffsets()
    currentTopicOffsets = Some(latest.topicOffsets)
    logDebug(s"GetOffset: ${latest.topicOffsets.toSeq.map(_.toString).sorted}")
    Some(latest.asInstanceOf[Offset])
  }

  override def latestOffset(startingOffset: streaming.Offset, readLimit: ReadLimit): streaming.Offset = {
    initialTopicOffsets
    val latestOffsets = pulsarHelper.fetchLatestOffsets().topicOffsets
    // add new partitions from PulsarAdmin, set to earliest entry and ledger id based on limit
    val existingStartOffsets = if (startingOffset != null) {
      getTopicOffsets(startingOffset.asInstanceOf[SpecificPulsarOffset])
    } else {
      Map[String, MessageId]()
    }
    print(s"readLimit: ${readLimit.toString}\n")
    val newTopics = latestOffsets.keySet.diff(existingStartOffsets.keySet)
    val startPartitionOffsets = existingStartOffsets ++ newTopics.map(topicPartition => topicPartition -> MessageId.earliest)
    val totalReadLimit = AdmissionLimits(readLimit).get.bytesToTake
    val offsets = mutable.Map[String, MessageId]()
    val numPartitions = startPartitionOffsets.size
    startPartitionOffsets.keys.foreach { topicPartition =>
      var readLimit = totalReadLimit / numPartitions
      pulsarHelper.fetchLatestOffsetForTopic(topicPartition)
      val messageId = startPartitionOffsets.apply(topicPartition)
      val ledgerId = getLedgerId(messageId)
      val entryId = getEntryId(messageId)
      pulsarAdmin.topics().getPartitionedInternalStats(topicPartition).partitions.forEach { (_, partitionMetadata) =>
        partitionMetadata.ledgers.sort((ledger1, ledger2) => {
          (ledger1.ledgerId - ledger2.ledgerId).toInt
        })
        partitionMetadata.ledgers.forEach { ledger =>
          if (ledger.ledgerId >= ledgerId) {
            val avgBytesPerEntries = ledger.size / ledger.entries
            // approximation of bytes left in ledger to deal with case
            // where we are at the middle of the ledger
            val bytesLeftInLedger = avgBytesPerEntries * {
              if (ledger.ledgerId == ledgerId) {
                ledger.entries - entryId
              } else {
                ledger.entries
              }
            }
            if (readLimit > bytesLeftInLedger) {
              readLimit -= bytesLeftInLedger
              offsets += (topicPartition -> DefaultImplementation
                .getDefaultImplementation
                .newMessageId(ledger.ledgerId, ledger.entries, -1))
            } else {
              offsets += (topicPartition -> DefaultImplementation
                .getDefaultImplementation
                .newMessageId(ledger.ledgerId, entryId + readLimit / avgBytesPerEntries, -1))
              readLimit = 0
            }
          }
        }
      }
    }
    SpecificPulsarOffset(offsets.toMap)
  }

  class AdmissionLimits(var bytesToTake: Long)

  object AdmissionLimits {
    def apply(limit: ReadLimit): Option[AdmissionLimits] = limit match {
      case maxBytes: ReadMaxBytes => Some(new AdmissionLimits(maxBytes.maxBytes))
      case _  : ReadAllAvailable => Some(new AdmissionLimits(Int.MaxValue))
    }

  }

  override def getBatch(start: Option[Offset], end: Offset): DataFrame = {
    // Make sure initialTopicOffsets is initialized
    initialTopicOffsets

    logInfo(s"getBatch called with start = $start, end = $end")
    val endTopicOffsets = SpecificPulsarOffset.getTopicOffsets(end)

    if (currentTopicOffsets.isEmpty) {
      currentTopicOffsets = Some(endTopicOffsets)
    }

    if (start.isDefined && start.get == end) {
      return sqlContext.internalCreateDataFrame(
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"),
        schema,
        isStreaming = true)
    }

    val fromTopicOffsets = start match {
      case Some(prevBatchEndOffset) =>
        SpecificPulsarOffset.getTopicOffsets(prevBatchEndOffset)
      case None =>
        initialTopicOffsets.topicOffsets
    }

    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.length

    val newTopics = endTopicOffsets.keySet.diff(fromTopicOffsets.keySet)
    val newTopicOffsets = pulsarHelper.fetchEarliestOffsets(newTopics.toSeq)

    val deletedPartitions = fromTopicOffsets.keySet.diff(endTopicOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"$deletedPartitions are gone. Some data may have been missed")
    }

    val newFromTopicOffsets = fromTopicOffsets ++ newTopicOffsets

    val offsetRanges = endTopicOffsets.keySet
      .map { tp =>
        val fromOffset = newFromTopicOffsets.getOrElse(
          tp, {
            // This should never happen
            throw new IllegalStateException(s"$tp doesn't have a from offset")
          })
        val untilOffset = endTopicOffsets(tp)
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

    val rdd = new PulsarSourceRDD(
      sc,
      new SchemaInfoSerializable(pulsarSchema),
      clientConf,
      readerConf,
      offsetRanges,
      pollTimeoutMs,
      failOnDataLoss,
      subscriptionNamePrefix,
      jsonOptions)

    logInfo(
      "GetBatch generating RDD of offset range: " +
        offsetRanges.sortBy(_.topic).mkString(", "))

    sqlContext.internalCreateDataFrame(rdd.setName("pulsar"), schema(), isStreaming = true)
  }

  override def commit(end: Offset): Unit = {
    val off = SpecificPulsarOffset.getTopicOffsets(end)
    pulsarHelper.commitCursorToOffset(off)
  }

  override def stop(): Unit = synchronized {
    if (!stopped) {
      pulsarHelper.removeCursor()
      pulsarHelper.close()
      stopped = true
    }

  }
}

/** A read limit that admits a soft-max of `maxBytes` per micro-batch. */
case class ReadMaxBytes(maxBytes: Long) extends ReadLimit