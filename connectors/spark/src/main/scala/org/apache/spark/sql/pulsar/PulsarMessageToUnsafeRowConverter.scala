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

import java.sql.Timestamp

import org.apache.pulsar.client.api.Message

import org.apache.spark.sql.catalyst.expressions.UnsafeRow
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.unsafe.types.UTF8String

/**
 * A simple class for converting pulsar Message to UnsafeRow.
 */
private[pulsar] class PulsarMessageToUnsafeRowConverter {

  private val rowWriter = new UnsafeRowWriter(PulsarReader.NUM_FIELDS)

  def toUnsafeRow(message: Message[Array[Byte]]): UnsafeRow = {
    rowWriter.reset()

    // key
    if (message.hasKey) {
      rowWriter.write(0, message.getKeyBytes)
    } else {
      rowWriter.setNullAt(0)
    }
    // value
    rowWriter.write(1, message.getValue)
    // topic
    rowWriter.write(2, UTF8String.fromString(message.getTopicName))
    // messageId
    rowWriter.write(3, message.getMessageId.toByteArray)
    // publish time
    rowWriter.write(
      4,
      DateTimeUtils.fromJavaTimestamp(new Timestamp(message.getPublishTime)))
    // event time
    if (message.getEventTime > 0) {
      rowWriter.write(
        5,
        DateTimeUtils.fromJavaTimestamp(new Timestamp(message.getEventTime)))
    } else {
      rowWriter.setNullAt(5)
    }
    rowWriter.getRow
  }

}
