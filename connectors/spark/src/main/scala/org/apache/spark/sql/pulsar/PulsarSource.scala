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

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.common.schema.SchemaInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.apache.spark.sql.execution.streaming.{Offset, Source}
import org.apache.spark.sql.types.StructType

private[pulsar] class PulsarSource(
    sqlContext: SQLContext,
    metadataReader: PulsarMetadataReader,
    clientConf: ju.Map[String, Object],
    consumerConf: ju.Map[String, Object],
    metadataPath: String,
    startingOffsets: SpecificPulsarOffset,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String)
  extends Source with Logging {

  import PulsarSourceUtils._

  private val sc = sqlContext.sparkContext

  val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  private lazy val initialTopicOffsets: SpecificPulsarOffset = {
    val metadataLog = new PulsarSourceInitialOffsetWriter(sqlContext.sparkSession, metadataPath)
    metadataLog.getInitialOffset(metadataReader, startingOffsets, reportDataLoss)
  }

  private var currentTopicOffsets: Option[Map[String, MessageId]] = None

  lazy val pulsarSchema: SchemaInfo = metadataReader.getPulsarSchema()

  override def schema(): StructType = SchemaUtils.pulsarSourceSchema(pulsarSchema)

  override def getOffset: Option[Offset] = {
    // Make sure initialTopicOffsets is initialized
    initialTopicOffsets
    val latest = metadataReader.fetchLatestOffsets()
    currentTopicOffsets = Some(latest.topicOffsets)
    logDebug(s"GetOffset: ${latest.topicOffsets.toSeq.map(_.toString).sorted}")
    Some(latest)
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
        sqlContext.sparkContext.emptyRDD[InternalRow].setName("empty"), schema, isStreaming = true)
    }

    val fromTopicOffsets = start match {
      case Some(prevBatchEndOffset) =>
        SpecificPulsarOffset.getTopicOffsets(prevBatchEndOffset)
      case None =>
        initialTopicOffsets.topicOffsets
    }

    val sortedExecutors = getSortedExecutorList(sc)
    val numExecutors = sortedExecutors.length

    val offsetRanges = endTopicOffsets.keySet.map { tp =>
      val fromOffset = fromTopicOffsets.getOrElse(tp, {
        // TODO: discover partition add and delete (for PartitionedTopic)
        // This should only happens when a new partition is added to a partitioned topic
        throw new IllegalStateException(s"A new topic $tp is added, it's not supported currently")
      })
      val untilOffset = endTopicOffsets(tp)
      val preferredLoc = if (numExecutors > 0) {
        // This allows cached PulsarClient in the executors to be re-used to read the same
        // partition in every batch.
        Some(sortedExecutors(Math.floorMod(tp.hashCode, numExecutors)))
      } else None
      PulsarOffsetRange(tp, fromOffset, untilOffset, preferredLoc)
    }.filter { range =>
      if (range.untilOffset.compareTo(range.fromOffset) < 0) {
        reportDataLoss(s"${range.topic}'s offset was changed " +
          s"from ${range.fromOffset} to ${range.untilOffset}, " +
          "some data might has been missed")
        false
      } else {
        true
      }
    }.toSeq

    val rdd = new PulsarSourceRDD(
      sc, new SchemaInfoSerializable(pulsarSchema),
      clientConf, consumerConf, offsetRanges, failOnDataLoss, subscriptionNamePrefix)

    logInfo("GetBatch generating RDD of offset range: " +
      offsetRanges.sortBy(_.topic).mkString(", "))

    sqlContext.internalCreateDataFrame(rdd.setName("pulsar"), schema, isStreaming = true)
  }

  override def commit(end: Offset): Unit = {
    val off = SpecificPulsarOffset.getTopicOffsets(end)
    metadataReader.commitCursorToOffset(off)
  }

  override def stop(): Unit = synchronized {
    metadataReader.removeCursor()
    metadataReader.close()
  }
}
