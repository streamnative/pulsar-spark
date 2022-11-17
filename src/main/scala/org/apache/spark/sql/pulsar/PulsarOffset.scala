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

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.impl.MessageIdImpl

import org.apache.spark.sql.connector.read.streaming.PartitionOffset
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}

private[pulsar] sealed trait PulsarOffset

private[pulsar] case object EarliestOffset extends PulsarOffset

private[pulsar] case object LatestOffset extends PulsarOffset

private[pulsar] case class TimeOffset(ts: Long) extends PulsarOffset

private[pulsar] sealed trait PerTopicOffset extends PulsarOffset

private[pulsar] case class SpecificPulsarOffset(topicOffsets: Map[String, MessageId])
    extends Offset
    with PerTopicOffset {

  override val json = JsonUtils.topicOffsets(topicOffsets)
}

private[pulsar] case class SpecificPulsarTime(topicTimes: Map[String, Long])
    extends Offset
    with PerTopicOffset {

  override def json(): String = JsonUtils.topicTimes(topicTimes)
}

private[pulsar] case class PulsarPartitionOffset(topic: String, messageId: MessageId)
    extends PartitionOffset

private[pulsar] object SpecificPulsarOffset {

  def getTopicOffsets(offset: Offset): Map[String, MessageId] = {
    offset match {
      case o: SpecificPulsarOffset => o.topicOffsets
      case so: SerializedOffset => SpecificPulsarOffset(so).topicOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to PulsarSourceOffset")
    }
  }

  def apply(offset: SerializedOffset): SpecificPulsarOffset =
    SpecificPulsarOffset(JsonUtils.topicOffsets(offset.json))

  def apply(offsetTuples: (String, MessageId)*): SpecificPulsarOffset = {
    SpecificPulsarOffset(offsetTuples.toMap)
  }
}

private[pulsar] case class UserProvidedMessageId(mid: MessageId)
    extends MessageIdImpl(
      mid.asInstanceOf[MessageIdImpl].getLedgerId,
      mid.asInstanceOf[MessageIdImpl].getEntryId,
      mid.asInstanceOf[MessageIdImpl].getPartitionIndex)
