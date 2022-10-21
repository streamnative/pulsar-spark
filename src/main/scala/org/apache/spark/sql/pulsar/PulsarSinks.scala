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
import java.util.concurrent.TimeUnit

import org.apache.pulsar.client.api.{Producer, Schema}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.expressions
import org.apache.spark.sql.catalyst.expressions.{Attribute, Literal}
import org.apache.spark.sql.execution.QueryExecution
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

private[pulsar] class PulsarSink(
    sqlContext: SQLContext,
    pulsarClientConf: ju.Map[String, Object],
    pulsarProducerConf: ju.Map[String, Object],
    topic: Option[String],
    adminUrl: String)
    extends Sink
    with Logging {

  @volatile private var latestBatchId = -1L

  override def toString: String = "PulsarSink"

  override def addBatch(batchId: Long, data: DataFrame): Unit = {
    PulsarSinks.validateQuery(data.schema.toAttributes, topic)

    if (batchId <= latestBatchId) {
      logInfo(s"Skipping already committed batch $batchId")
    } else {
      PulsarSinks.write(
        sqlContext.sparkSession,
        data.queryExecution,
        pulsarClientConf,
        pulsarProducerConf,
        topic,
        adminUrl)
      latestBatchId = batchId
    }
  }
}

private[pulsar] object PulsarSinks extends Logging {

  import PulsarOptions._

  def checkForUnsupportedType(valuesDT: Seq[DataType]): Unit = {
    valuesDT.map { dt =>
      dt match {
        case CalendarIntervalType =>
          throw new AnalysisException("CalendarIntervalType not supported by pulsar sink yet")
        case u: UserDefinedType[_] =>
          throw new AnalysisException(s"$u not supported by pulsar sink yet")
        case o: ObjectType => throw new AnalysisException(s"$o not supported by pulsar sink yet")
        case st: StructType => checkForUnsupportedType(st.fields.map(_.dataType).toSeq)
        case _ => // spark types we are able to handle right now
      }
    }
  }

  def validateQuery(schema: Seq[Attribute], topic: Option[String]): Unit = {
    schema
      .find(_.name == TopicAttributeName)
      .getOrElse(topic match {
        case Some(topicValue) => Literal(UTF8String.fromString(topicValue), StringType)
        case None =>
          throw new AnalysisException(
            s"topic option required when no " +
              s"'$TopicAttributeName' attribute is present. Use the " +
              s"$TopicSingle option for setting a topic.")
      })
      .dataType match {
      case StringType => // good
      case _ =>
        throw new AnalysisException(s"Topic type must be a ${StringType.catalogString}")
    }

    schema
      .find(_.name == PulsarOptions.KeyAttributeName)
      .getOrElse(Literal(null, StringType))
      .dataType match {
      case StringType | BinaryType => // good
      case _ =>
        throw new AnalysisException(
          s"${PulsarOptions.KeyAttributeName} attribute type " +
            s"must be a ${StringType.catalogString} or ${BinaryType.catalogString}")
    }

    schema
      .find(_.name == PulsarOptions.EventTimeName)
      .getOrElse(Literal(null, LongType))
      .dataType match {
      case LongType | TimestampType => // good
      case _ =>
        throw new AnalysisException(
          s"${PulsarOptions.EventTimeName} attribute type " +
            s"must be a ${LongType.catalogString} or ${TimestampType.catalogString}")
    }

    schema
      .find(a =>
        a.name == PulsarOptions.MessageIdName ||
          a.name == PulsarOptions.PublishTimeName)
      .map(a =>
        logWarning(s"${a.name} attribute exists in schema," +
          "it's reserved by Pulsar Source and generated automatically by pulsar for each record." +
          "Choose another name if you want to keep this field or it will be ignored by pulsar."))

    val valuesExpression =
      schema.filter(n => !PulsarOptions.MetaFieldNames.contains(n.name))

    if (valuesExpression.length == 0) {
      throw new AnalysisException("Schema should have at least one non-key/non-topic field")
    }

    checkForUnsupportedType(valuesExpression.map(_.dataType))
  }

  def write(
      sparkSession: SparkSession,
      queryExecution: QueryExecution,
      pulsarClientConf: ju.Map[String, Object],
      pulsarProducerConf: ju.Map[String, Object],
      topic: Option[String],
      adminUrl: String): Unit = {

    // validate the schema
    val schema = queryExecution.analyzed.output
    validateQuery(schema, topic)

    // execute RDD
    queryExecution.toRdd.foreachPartition { iter =>
      val writeTask =
        new PulsarWriteTask(pulsarClientConf, pulsarProducerConf, topic, schema, adminUrl)
      Utils.tryWithSafeFinally(block = writeTask.execute(iter))(finallyBlock = writeTask.close())
    }
  }

  def createProducer[T](
      clientConf: ju.Map[String, Object],
      producerConf: ju.Map[String, Object],
      topic: String,
      schema: Schema[T]): Producer[T] = {

    CachedPulsarClient
      .getOrCreate(clientConf)
      .newProducer(schema)
      .topic(topic)
      .loadConf(producerConf)
      .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
      // maximizing the throughput
      .batchingMaxMessages(5 * 1024 * 1024)
      .create()
  }

  def toStructType(attrs: Seq[Attribute]): StructType = {
    expressions.AttributeSeq(attrs).toStructType
  }
}
