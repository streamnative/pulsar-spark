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
import java.util.function.BiConsumer

import scala.collection.mutable

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{MessageId, Producer}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, Cast, Literal, UnsafeProjection}
import org.apache.spark.sql.types._

private[pulsar] class PulsarWriteTask(
    clientConf: ju.Map[String, Object],
    producerConf: ju.Map[String, Object],
    topic: Option[String],
    inputSchema: Seq[Attribute],
    adminUrl: String)
    extends PulsarRowWriter(inputSchema, clientConf, producerConf, topic, adminUrl) {

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
    topic: Option[String],
    adminUrl: String) {

  import PulsarOptions._

  protected lazy val admin = AdminUtils.buildAdmin(adminUrl, clientConf)

  private def createProjections = {
    val topicExpression = topic
      .map(Literal(_))
      .orElse {
        inputSchema.find(_.name == TopicAttributeName)
      }
      .getOrElse {
        throw new IllegalStateException(
          s"topic option required when no " +
            s"'$TopicAttributeName' attribute is present")
      }
    topicExpression.dataType match {
      case StringType => // good
      case t =>
        throw new IllegalStateException(
          TopicAttributeName +
            s"attribute unsupported type $t. $TopicAttributeName " +
            s"must be a ${StringType.catalogString}")
    }

    val keyExpression = inputSchema
      .find(_.name == KeyAttributeName)
      .getOrElse(Literal(null, BinaryType))
    keyExpression.dataType match {
      case StringType | BinaryType => // good
      case t =>
        throw new IllegalStateException(
          KeyAttributeName +
            s"attribute unsupported type ${t.catalogString}")
    }

    val eventTimeExpression = inputSchema
      .find(_.name == EventTimeName)
      .getOrElse(Literal(null, LongType))
    eventTimeExpression.dataType match {
      case LongType | TimestampType => // good
      case t =>
        throw new IllegalStateException(
          EventTimeName +
            s"attribute unsupported type ${t.catalogString}")
    }

    val metaProj = UnsafeProjection.create(
      Seq(topicExpression, Cast(keyExpression, BinaryType), eventTimeExpression),
      inputSchema)

    val valuesExpression =
      inputSchema.filter(n => !PulsarOptions.MetaFieldNames.contains(n.name))

    val valueProj = UnsafeProjection.create(valuesExpression, inputSchema)

    val isStruct = if (valuesExpression.length == 1) {
      valuesExpression.head.dataType match {
        case st: StructType => true
        case _ => false
      }
    } else true

    (valuesExpression, metaProj, valueProj, isStruct)
  }

  // used to synchronize with Pulsar callbacks
  @volatile protected var failedWrite: Throwable = _

  protected val (valueSchema, metaProj, valueProj, valIsStruct) = createProjections

  protected lazy val dataType =
    if (valIsStruct) PulsarSinks.toStructType(valueSchema) else valueSchema.head.dataType

  protected lazy val pulsarSchema = SchemaUtils.sqlType2PSchema(dataType)
  protected lazy val serializer = new PulsarSerializer(dataType, false)

  // reuse producer through the executor
  protected lazy val singleProducer =
    if (topic.isDefined) {
      SchemaUtils.uploadPulsarSchema(admin, topic.get, pulsarSchema.getSchemaInfo)
      PulsarSinks.createProducer(clientConf, producerConf, topic.get, pulsarSchema)
    } else null
  protected val topic2Producer: mutable.Map[String, Producer[_]] = mutable.Map.empty

  def getProducer[T](tp: String): Producer[T] = {
    if (null != singleProducer) {
      return singleProducer.asInstanceOf[Producer[T]]
    }

    if (topic2Producer.contains(tp)) {
      topic2Producer(tp).asInstanceOf[Producer[T]]
    } else {
      SchemaUtils.uploadPulsarSchema(admin, tp, pulsarSchema.getSchemaInfo)
      val p =
        PulsarSinks.createProducer(clientConf, producerConf, tp, pulsarSchema)
      topic2Producer.put(tp, p)
      p.asInstanceOf[Producer[T]]
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
   * Send the specified row to the producer, with a callback that will save any exception to
   * failedWrite. Note that send is asynchronous; subclasses must flush() their producer before
   * assuming the row is in Pulsar.
   */
  protected def sendRow(row: InternalRow): Unit = {
    val metaRow = metaProj(row)
    val valueRow = valueProj(row)
    val value = serializer.serialize(valueRow)

    val topic = metaRow.getUTF8String(0)
    val key = metaRow.getBinary(1)

    if (topic == null) {
      throw new NullPointerException(
        s"null topic present in the data. Use the " +
          s"$TopicSingle option for setting a topic.")
    }

    val mb = getProducer(topic.toString).newMessage().value(value)
    if (null != key) {
      mb.keyBytes(key)
    }

    if (!metaRow.isNullAt(2)) {
      val eventTime = metaRow.getLong(2)
      if (eventTime > 0) {
        mb.eventTime(eventTime)
      }
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
    admin.close()
  }
}
