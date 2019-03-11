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

import java.util.concurrent.TimeUnit
import java.util.function.BiConsumer
import java.{util => ju}

import org.apache.pulsar.client.api.{MessageId, Producer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.types.{BinaryType, StringType}

private[pulsar] class PulsarWriteTask(
  pulsarConf: ju.Map[String, Object],
  inputSchema: Seq[Attribute],
  topic: String) extends PulsarRowWriter(inputSchema, topic) {

  lazy val producer = CachedPulsarClient.getOrCreate(pulsarConf)
    .newProducer(org.apache.pulsar.client.api.Schema.BYTES)
    .topic(topic)
    .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
    // maximizing the throughput
    .batchingMaxMessages(5 * 1024 * 1024)
    .create();

  /**
    * Writes key value data out to topics.
    */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      sendRow(currentRow, producer)
    }
  }

  def close(): Unit = {
    checkForErrors()
    if (producer != null) {
      producer.flush()
      checkForErrors()
    }
  }

}

private[pulsar] abstract class PulsarRowWriter(
    inputSchema: Seq[Attribute], topic: String) {

  // used to synchronize with Pulsar callbacks
  @volatile protected var failedWrite: Throwable = _
  protected val projection = createProjection

  private val sendCallback = new BiConsumer[MessageId, Throwable]() {
    override def accept(t: MessageId, u: Throwable): Unit = {
      if (failedWrite == null && u != null) {
        failedWrite = u
      }
    }
  }

  /**
    * Send the specified row to the producer, with a callback that will save any exception
    * to failedWrite. Note that send is asynchronous; subclasses must flush() their producer before
    * assuming the row is in Pulsar.
    */
  protected def sendRow(
      row: InternalRow, producer: Producer[Array[Byte]]): Unit = {
    val projectedRow = projection(row)
    val key = projectedRow.getBinary(0)
    val value = projectedRow.getBinary(1)
    producer.newMessage()
      .keyBytes(key)
      .value(value)
      .sendAsync()
      .whenComplete(sendCallback)
  }

  protected def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }

  private def createProjection = {
    val keyExpression = inputSchema.find(_.name == PulsarOptions.KEY_ATTRIBUTE_NAME)
      .getOrElse(Literal(null, BinaryType))
    keyExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"${PulsarOptions.KEY_ATTRIBUTE_NAME} " +
          s"attribute unsupported type ${t.catalogString}")
    }
    val valueExpression = inputSchema
      .find(_.name == PulsarOptions.VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException("Required attribute " +
        s"'${PulsarOptions.VALUE_ATTRIBUTE_NAME}' not found")
    )
    valueExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(s"${PulsarOptions.VALUE_ATTRIBUTE_NAME} " +
          s"attribute unsupported type ${t.catalogString}")
    }
    UnsafeProjection.create(
      Seq(Cast(keyExpression, BinaryType), Cast(valueExpression, BinaryType)), inputSchema)
  }
}
