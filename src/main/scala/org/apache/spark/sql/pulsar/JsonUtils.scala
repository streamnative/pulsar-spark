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
import org.json4s.{Formats, NoTypeHints}
import org.json4s.jackson.Serialization

/**
 * Utils for converting pulsar objects to and from json.
 */
object JsonUtils {

  private implicit val formats: AnyRef with Formats = Serialization.formats(NoTypeHints)

  def topics(str: String): Array[String] = {
    Serialization.read[Array[String]](str)
  }

  def topics(topics: Array[String]): String = {
    Serialization.write(topics)
  }

  def topicOffsets(str: String): Map[String, MessageId] = {
    Serialization.read[Map[String, Array[Byte]]](str).map { case (topic, msgIdBytes) =>
      (topic, MessageId.fromByteArray(msgIdBytes))
    }
  }

  def topicOffsets(topicOffsets: Map[String, MessageId]): String = {
    Serialization.write(topicOffsets.map { case (topic, msgId) =>
      (topic, msgId.toByteArray)
    })
  }

  def topicTimes(topicTimes: Map[String, Long]): String = {
    Serialization.write(topicTimes)
  }

  def topicTimes(str: String): Map[String, Long] = {
    Serialization.read[Map[String, Long]](str)
  }
}
