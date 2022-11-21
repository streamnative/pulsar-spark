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

import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.sources.{BaseRelation, TableScan}
import org.apache.spark.sql.types.StructType

/**
 * Read Pulsar in batch mode
 */
private[pulsar] class PulsarRelation(
    override val sqlContext: SQLContext,
    override val schema: StructType,
    schemaInfo: SchemaInfoSerializable,
    adminUrl: String,
    clientConf: ju.Map[String, Object],
    readerConf: ju.Map[String, Object],
    startingOffset: SpecificPulsarOffset,
    endingOffset: SpecificPulsarOffset,
    pollTimeoutMs: Int,
    failOnDataLoss: Boolean,
    subscriptionNamePrefix: String,
    jsonOptions: JSONOptionsInRead)
    extends BaseRelation
    with TableScan
    with Logging {

  import PulsarSourceUtils._

  private val reportDataLoss = reportDataLossFunc(failOnDataLoss)

  override def buildScan(): RDD[Row] = {
    val fromTopicOffsets = startingOffset.topicOffsets
    val endTopicOffsets = endingOffset.topicOffsets

    if (fromTopicOffsets.keySet != endTopicOffsets.keySet) {
      val fromTopics = fromTopicOffsets.keySet.toList.sorted.mkString(",")
      val endTopics = endTopicOffsets.keySet.toList.sorted.mkString(",")
      throw new IllegalStateException(
        "different topics " +
          s"for starting offsets topics[${fromTopics}] and " +
          s"ending offsets topics[${endTopics}]")
    }

    val offsetRanges = endTopicOffsets.keySet
      .map { tp =>
        val fromOffset = fromTopicOffsets.getOrElse(
          tp, {
            // this shouldn't happen since we had checked it
            throw new IllegalStateException(s"$tp doesn't have a from offset")
          })
        val untilOffset = endTopicOffsets(tp)
        PulsarOffsetRange(tp, fromOffset, untilOffset, None)
      }
      .filter { range =>
        if (range.untilOffset.compareTo(range.fromOffset) < 0) {
          reportDataLoss(
            s"${range.topic}'s offset was changed " +
              s"from ${range.fromOffset} to ${range.untilOffset}, " +
              "some data might has been missed")
          false
        } else {
          true
        }
      }
      .toSeq

    val rdd = new PulsarSourceRDD4Batch(
      sqlContext.sparkContext,
      schemaInfo,
      adminUrl,
      clientConf,
      readerConf,
      offsetRanges,
      pollTimeoutMs,
      failOnDataLoss,
      subscriptionNamePrefix,
      jsonOptions)
    sqlContext.internalCreateDataFrame(rdd.setName("pulsar"), schema).rdd
  }
}
