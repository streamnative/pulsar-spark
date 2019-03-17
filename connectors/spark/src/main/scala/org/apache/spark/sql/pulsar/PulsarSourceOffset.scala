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

import org.apache.pulsar.client.api.MessageId
import org.apache.spark.sql.execution.streaming.{Offset, SerializedOffset}
import org.apache.spark.sql.sources.v2.reader.streaming.{PartitionOffset, Offset => OffsetV2}

private[pulsar] case class PulsarSourceOffset(topicOffsets: Map[String, MessageId])
  extends OffsetV2 {

  override val json = JsonUtils.topicOffsets(topicOffsets)

}

private[pulsar] case class PulsarSourcePartitionOffset(topic: String, messageId: MessageId)
  extends PartitionOffset

private[pulsar] object PulsarSourceOffset {

  def getTopicOffsets(offset: Offset): Map[String, MessageId] = {
    offset match {
      case o: PulsarSourceOffset => o.topicOffsets
      case so: SerializedOffset => PulsarSourceOffset(so).topicOffsets
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid conversion from offset of ${offset.getClass} to PulsarSourceOffset")
    }
  }

  def apply(offset: SerializedOffset): PulsarSourceOffset =
    PulsarSourceOffset(JsonUtils.topicOffsets(offset.json))

}
