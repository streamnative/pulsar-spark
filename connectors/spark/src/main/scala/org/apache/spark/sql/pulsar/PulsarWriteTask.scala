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

import java.util.function.BiConsumer
import java.{util => ju}

import scala.collection.mutable

import org.apache.pulsar.client.api.{MessageId, Producer}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.types.{BinaryType, StringType}

private[pulsar] class PulsarWriteTask(
    clientConf: ju.Map[String, Object],
    producerConf: ju.Map[String, Object],
    topic: Option[String],
    inputSchema: Seq[Attribute]) extends PulsarRowWriter(inputSchema, clientConf, producerConf, topic) {

  /**
    * Writes key value data out to topics.
    */
  def execute(iterator: Iterator[InternalRow]): Unit = {
    while (iterator.hasNext && failedWrite == null) {
      val currentRow = iterator.next()
      sendRow(currentRow)
    }
  }

  def close(): Unit = {
    checkForErrors()
    producerClose()
    checkForErrors()
  }

}

private[pulsar] abstract class PulsarRowWriter(
    inputSchema: Seq[Attribute],
    clientConf: ju.Map[String, Object],
    producerConf: ju.Map[String, Object],
    topic: Option[String]) {
  
  import PulsarOptions._

  // used to synchronize with Pulsar callbacks
  @volatile protected var failedWrite: Throwable = _
  protected val projection = createProjection

  // reuse producer through the executor
  protected lazy val singleProducer =
    if (topic.isDefined) {
      CachedPulsarProducer.getOrCreate((clientConf, producerConf, topic.get))
    } else null
  protected val topic2Producer: mutable.Map[String, Producer[Array[Byte]]] = mutable.Map.empty

  def getProducer(tp: String): Producer[Array[Byte]] = {
    if (null != singleProducer) {
      return singleProducer
    }

    if (topic2Producer.contains(tp)) {
      topic2Producer(tp)
    } else {
      val p =
        CachedPulsarProducer.getOrCreate((clientConf, producerConf, tp))
      topic2Producer.put(tp, p)
      p
    }
  }

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
  protected def sendRow(row: InternalRow): Unit = {
    val projectedRow = projection(row)
    val topic = projectedRow.getUTF8String(0)
    val key = projectedRow.getBinary(1)
    val value = projectedRow.getBinary(2)

    if (topic == null) {
      throw new NullPointerException(s"null topic present in the data. Use the " +
        s"$TOPIC_SINGLE option for setting a topic.")
    }

    val mb = getProducer(topic.toString).newMessage().value(value)
    if (null != key) {
      mb.keyBytes(key)
    }
    mb.sendAsync().whenComplete(sendCallback)
  }

  protected def checkForErrors(): Unit = {
    if (failedWrite != null) {
      throw failedWrite
    }
  }

  protected def producerFlush(): Unit = {
    if (singleProducer != null) {
      singleProducer.flush()
    } else {
      topic2Producer.foreach(_._2.flush())
    }
  }

  protected def producerClose(): Unit = {
    producerFlush()
    topic2Producer.clear()
  }

  private def createProjection = {
    val topicExpression = topic.map(Literal(_)).orElse {
      inputSchema.find(_.name == TOPIC_ATTRIBUTE_NAME)
    }.getOrElse {
      throw new IllegalStateException(s"topic option required when no " +
        s"'$TOPIC_ATTRIBUTE_NAME' attribute is present")
    }
    topicExpression.dataType match {
      case StringType => // good
      case t =>
        throw new IllegalStateException(TOPIC_ATTRIBUTE_NAME +
          s"attribute unsupported type $t. $TOPIC_ATTRIBUTE_NAME " +
          s"must be a ${StringType.catalogString}")
    }

    val keyExpression = inputSchema.find(_.name == KEY_ATTRIBUTE_NAME)
      .getOrElse(Literal(null, BinaryType))
    keyExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(KEY_ATTRIBUTE_NAME +
          s"attribute unsupported type ${t.catalogString}")
    }
    val valueExpression = inputSchema
      .find(_.name == VALUE_ATTRIBUTE_NAME).getOrElse(
      throw new IllegalStateException("Required attribute " +
        s"'$VALUE_ATTRIBUTE_NAME' not found")
    )
    valueExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(VALUE_ATTRIBUTE_NAME +
          s"attribute unsupported type ${t.catalogString}")
    }
    UnsafeProjection.create(
      Seq(
        topicExpression,
        Cast(keyExpression, BinaryType),
        Cast(valueExpression, BinaryType)), inputSchema)
  }
}
