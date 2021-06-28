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

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.impl.MessageIdImpl
import org.apache.pulsar.common.schema.SchemaInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReaderFactory}
import org.apache.spark.sql.connector.read.streaming.{MicroBatchStream, Offset}

import PulsarSourceUtils._

class PulsarMicroBatchStream(
                              metadataReader: PulsarMetadataReader,
                              clientConf: ju.Map[String, Object],
                              readerConf: ju.Map[String, Object],
                              metadataPath: String,
                              startingOffsets: PerTopicOffset,
                              pollTimeoutMs: Int,
                              failOnDataLoss: Boolean,
                              subscriptionNamePrefix: String,
                              jsonOptions: JSONOptionsInRead
                            ) extends MicroBatchStream with Logging {


  lazy val pulsarSchema: SchemaInfo = {
    val tpVersionOpt = jsonOptions.parameters.get(PulsarOptions.TOPIC_VERSION)
    metadataReader.getPulsarSchema(tpVersionOpt)
  }

  lazy val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  override def initialOffset(): Offset = {
    val metadataLog =
      new PulsarSourceInitialOffsetWriter(
        SparkSession.getActiveSession.getOrElse(throw new RuntimeException("No active SparkSession found !")),
        metadataPath)
    metadataLog.getInitialOffset(
      metadataReader,
      startingOffsets,
      pollTimeoutMs,
      reportDataLoss)
  }

  override def latestOffset(): Offset =
    SpecificPulsarOffset(metadataReader.fetchLatestOffsets().topicOffsets)

  override def planInputPartitions(start: Offset, `end`: Offset): Array[InputPartition] = {

    val startPartitionOffsets = start.asInstanceOf[SpecificPulsarOffset].topicOffsets
    val endPartitionOffsets = end.asInstanceOf[SpecificPulsarOffset].topicOffsets
    val newPartitions = endPartitionOffsets.keySet.diff(startPartitionOffsets.keySet)
    val newPartitionInitialOffsets = metadataReader.fetchEarliestOffsets(newPartitions.toSeq)
    logInfo(s"Topics added: $newPartitions")

    val deletedPartitions = startPartitionOffsets.keySet.diff(endPartitionOffsets.keySet)
    if (deletedPartitions.nonEmpty) {
      reportDataLoss(s"$deletedPartitions are gone. Some data may have been missed")
    }

    val newStartsOffsets: Map[String, MessageId] =
      startPartitionOffsets ++ newPartitionInitialOffsets

    val offsetRanges = endPartitionOffsets.keySet
      .map { tp =>
        val fromOffset = newStartsOffsets.getOrElse(tp, {
          // this shouldn't happen
          throw new IllegalStateException(s"$tp doesn't have a start offset")
        })
        val untilOffset = endPartitionOffsets(tp)
        val sortedExecutors = getSortedExecutorList()
        val numExecutors = sortedExecutors.length
        val preferredLoc = if (numExecutors > 0) {
          // This allows cached PulsarClient in the executors to be re-used to read the same
          // partition in every batch.
          Some(sortedExecutors(Math.floorMod(tp.hashCode, numExecutors)))
        } else None
        PulsarOffsetRange(tp, fromOffset, untilOffset, preferredLoc)
      }
      .filter {range =>
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
      }.toSeq

    try {
      metadataReader.commitCursorToOffset(endPartitionOffsets)
    } catch {
      case e: Exception => logWarning(s"Exception while commit cursor to offset $endPartitionOffsets. ${e.getMessage}")
    }

    offsetRanges.map { range =>
      PulsarBatchInputPartition(
        range,
        new SchemaInfoSerializable(pulsarSchema),
        jsonOptions.parameters.get(PulsarOptions.ADMIN_URL_OPTION_KEY).get,
        clientConf,
        readerConf,
        pollTimeoutMs,
        failOnDataLoss,
        subscriptionNamePrefix,
        jsonOptions
      )
    }.toArray
  }

  override def createReaderFactory(): PartitionReaderFactory = PulsarBatchReaderFactory

  override def deserializeOffset(json: String): Offset =
    SpecificPulsarOffset(JsonUtils.topicOffsets(json))

  override def commit(`end`: Offset): Unit = {}

  override def stop(): Unit = {
    logInfo("Stop PulsarMicroBatchStreamReader ..")
    if (!jsonOptions.parameters
      .get(PulsarOptions.RETAIN_SUBCRIPTION_OPTION_KEY)
      .getOrElse("false").toBoolean)
    {
      try {
        metadataReader.removeCursor()
      } catch {
        case e: Exception => logWarning(s"Exception while remove cursor ${e.getMessage}")
      }
    }
    metadataReader.close()
  }
}
