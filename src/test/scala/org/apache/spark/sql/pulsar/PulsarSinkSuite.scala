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

import java.text.SimpleDateFormat
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, Locale}

import scala.reflect.ClassTag

import org.scalatest.time.SpanSugar._

import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaInfo

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.streaming._
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.sql._

class PulsarSinkSuite extends StreamTest with SharedSQLContext with PulsarTest {
  import PulsarOptions._
  import SchemaData._
  import testImplicits._

  override val streamingTimeout = 30.seconds

  test("batch - write to pulsar") {
    val topic = newTopic()
    val df = Seq("1", "2", "3", "4", "5") map { v =>
      (v, v)
    } toDF ("key", "value")
    df.write
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)
      .save()

    checkAnswer(
      createPulsarReader(topic).selectExpr("CAST(value as STRING) value"),
      Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }

  private def batchCheck[T: ClassTag](
      schemaInfo: SchemaInfo,
      data: Seq[T],
      encoder: Encoder[T],
      str: T => String) = {
    val topic = newTopic()

    val df = if (str == null) {
      spark.createDataset(data)(encoder).toDF().selectExpr("CAST(value as String)")
    } else {
      spark.createDataset(data.map(str))(Encoders.STRING).toDF()
    }

    df.write
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)
      .save()

    val df1 = createPulsarReader(topic).selectExpr("CAST(value as STRING) value")

    checkAnswer(df1, df)
  }

  test("batch - boolean") {
    batchCheck[Boolean](Schema.BOOL.getSchemaInfo, booleanSeq, Encoders.scalaBoolean, null)
  }

  test("batch - int") {
    batchCheck[Int](Schema.INT32.getSchemaInfo, int32Seq, Encoders.scalaInt, null)
  }

  test("batch - string") {
    batchCheck[String](Schema.STRING.getSchemaInfo, stringSeq, Encoders.STRING, null)
  }

  test("batch - byte") {
    batchCheck[Byte](Schema.INT8.getSchemaInfo, int8Seq, Encoders.scalaByte, null)
  }

  test("batch - double") {
    batchCheck[Double](Schema.DOUBLE.getSchemaInfo, doubleSeq, Encoders.scalaDouble, null)
  }

  test("batch - float") {
    batchCheck[Float](Schema.FLOAT.getSchemaInfo, floatSeq, Encoders.scalaFloat, null)
  }

  test("batch - short") {
    batchCheck[Short](Schema.INT16.getSchemaInfo, int16Seq, Encoders.scalaShort, null)
  }

  test("batch - long") {
    batchCheck[Long](Schema.INT64.getSchemaInfo, int64Seq, Encoders.scalaLong, null)

  }

  test("batch - date") {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    batchCheck[Date](
      Schema.DATE.getSchemaInfo,
      dateSeq,
      Encoders.bean(classOf[Date]),
      dateFormat.format(_))
  }

  test("batch - timestamp") {
    val tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    batchCheck[java.sql.Timestamp](
      Schema.TIMESTAMP.getSchemaInfo,
      timestampSeq,
      Encoders.kryo(classOf[java.sql.Timestamp]),
      tsFormat.format(_))
  }

  test("batch - byte array") {
    batchCheck[Array[Byte]](Schema.BYTES.getSchemaInfo, bytesSeq, Encoders.BINARY, new String(_))
  }

  test("batch - struct types") {
    val topic = newTopic()

    val df = fooSeq.toDF()

    df.write
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)
      .save()

    val df1 = createPulsarReader(topic).selectExpr("i", "f", "bar")

    checkAnswer(df1, df)
  }

  test("batch - null topic field value, and no topic option") {
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")
    val ex = intercept[AnalysisException] {
      df.write
        .format("pulsar")
        .option(SERVICE_URL_OPTION_KEY, serviceUrl)
        .option(ADMIN_URL_OPTION_KEY, adminUrl)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("topic option required"))
  }

  test("batch - unsupported save modes") {
    val topic = newTopic()
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")

    // Test bad save mode Ignore
    var ex = intercept[AnalysisException] {
      df.write
        .format("pulsar")
        .option(SERVICE_URL_OPTION_KEY, serviceUrl)
        .mode(SaveMode.Ignore)
        .save()
    }
    assert(
      ex.getMessage.toLowerCase(Locale.ROOT).contains(s"save mode ignore not allowed for pulsar"))

    // Test bad save mode Overwrite
    ex = intercept[AnalysisException] {
      df.write
        .format("pulsar")
        .option(SERVICE_URL_OPTION_KEY, serviceUrl)
        .mode(SaveMode.Overwrite)
        .save()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains(s"save mode overwrite not allowed for pulsar"))
  }

  test("streaming - write to pulsar with topic field") {
    val input = MemoryStream[String]
    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = None,
      withOutputMode = Some(OutputMode.Append))(withSelectExpr = s"'$topic' as __topic", "value")

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(__key as STRING) __key", "CAST(value as STRING) value")
      .selectExpr("CAST(__key as INT) __key", "CAST(value as INT) value")
      .as[(Option[Int], Int)]
      .map(_._2)

    try {
      input.addData("1", "2", "3", "4", "5")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      input.addData("6", "7", "8", "9", "10")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      writer.stop()
    }
  }

  test("streaming - write aggregation w/o topic field, with topic option") {
    val input = MemoryStream[String]
    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF().groupBy("value").count(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Update()))(
      withSelectExpr = "CAST(value as STRING) __key",
      "CAST(count as STRING) value"
    )

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(__key as STRING) __key", "CAST(value as STRING) value")
      .selectExpr("CAST(__key as INT) __key", "CAST(value as INT) value")
      .as[(Int, Int)]

    try {
      input.addData("1", "2", "2", "3", "3", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3))

      input.addData("1", "2", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3), (1, 2), (2, 3), (3, 4))
    } finally {
      writer.stop()
    }
  }

  test("streaming - aggregation with topic field and topic option") {
    /* The purpose of this test is to ensure that the topic option
     * overrides the topic field. We begin by writing some data that
     * includes a topic field and value (e.g., 'foo') along with a topic
     * option. Then when we read from the topic specified in the option
     * we should see the data i.e., the data was written to the topic
     * option, and not to the topic in the data e.g., foo
     */
    val input = MemoryStream[String]
    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF().groupBy("value").count(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Update()))(
      withSelectExpr = "'foo' as __topic",
      "CAST(value as STRING) __key",
      "CAST(count as STRING) value")

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(__key AS INT)", "CAST(value AS INT)")
      .as[(Int, Int)]

    try {
      input.addData("1", "2", "2", "3", "3", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3))
      input.addData("1", "2", "3")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, (1, 1), (2, 2), (3, 3), (1, 2), (2, 3), (3, 4))
    } finally {
      writer.stop()
    }
  }

  private def streamCheck[T: ClassTag](
      schemaInfo: SchemaInfo,
      data: Seq[T],
      encoder: Encoder[T],
      str: T => String) = {
    implicit val enc = encoder
    val input = MemoryStream[T]
    val topic = newTopic()
    createPulsarSchema(topic, schemaInfo)

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append))(withSelectExpr = s"'$topic' as __topic", "value")

    try {
      input.addData(data)
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }

      val reader = createPulsarReader(topic)
        .selectExpr("CAST(value as STRING)")
        .as[String]

      if (str == null) {
        checkDatasetUnorderly[String](reader, data.map(_.toString): _*)
      } else {
        checkDatasetUnorderly[String](reader, data.map(str(_)): _*)
      }
    } finally {
      writer.stop()
    }
  }

  test("streaming - write bool") {
    streamCheck[Boolean](Schema.BOOL.getSchemaInfo, booleanSeq, Encoders.scalaBoolean, null)
  }

  test("streaming - write int") {
    streamCheck[Int](Schema.INT32.getSchemaInfo, int32Seq, Encoders.scalaInt, null)
  }

  test("streaming - write string") {
    streamCheck[String](Schema.STRING.getSchemaInfo, stringSeq, Encoders.STRING, null)
  }

  test("streaming - write byte") {
    streamCheck[Byte](Schema.INT8.getSchemaInfo, int8Seq, Encoders.scalaByte, null)
  }

  test("streaming - write double") {
    streamCheck[Double](Schema.DOUBLE.getSchemaInfo, doubleSeq, Encoders.scalaDouble, null)
  }

  test("streaming - write float") {
    streamCheck[Float](Schema.FLOAT.getSchemaInfo, floatSeq, Encoders.scalaFloat, null)
  }

  test("streaming - write short") {
    streamCheck[Short](Schema.INT16.getSchemaInfo, int16Seq, Encoders.scalaShort, null)
  }

  test("streaming - write long") {
    streamCheck[Long](Schema.INT64.getSchemaInfo, int64Seq, Encoders.scalaLong, null)
  }

  test("streaming - write byte array") {
    streamCheck[Array[Byte]](Schema.BYTES.getSchemaInfo, bytesSeq, Encoders.BINARY, new String(_))
  }

  test("streaming - write date") {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    streamCheck[java.sql.Date](
      Schema.DATE.getSchemaInfo,
      dateSeq.map(d => new java.sql.Date(d.getTime)),
      Encoders.DATE,
      dateFormat.format(_))
  }

  test("streaming - write timestamp") {
    val tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    streamCheck[java.sql.Timestamp](
      Schema.TIMESTAMP.getSchemaInfo,
      timestampSeq,
      Encoders.TIMESTAMP,
      tsFormat.format(_))
  }

  test("streaming - write struct type") {
    val input = MemoryStream[Foo]
    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append))(withSelectExpr = "i", "f", "bar")

    try {
      input.addData(fooSeq)
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }

      val reader = createPulsarReader(topic)
        .selectExpr("i", "f", "bar")
        .as[Foo]

      implicit val orderingB = new Ordering[Bar] {
        override def compare(x: Bar, y: Bar): Int = {
          Ordering.Tuple2[Boolean, String].compare((x.b, x.s), (y.b, y.s))
        }
      }

      implicit val orderingF = new Ordering[Foo] {
        override def compare(x: Foo, y: Foo): Int = {
          Ordering.Tuple3[Int, Float, Bar].compare((x.i, x.f, x.bar), (y.i, y.f, y.bar))
        }
      }

      checkDatasetUnorderly[Foo](reader, fooSeq: _*)

    } finally {
      writer.stop()
    }
  }

  test("streaming - write data with bad schema") {
    val input = MemoryStream[String]
    val topic = newTopic()

    var writer: StreamingQuery = null
    var ex: Exception = null

    try {
      /* No value field */
      ex = intercept[StreamingQueryException] {
        writer = createPulsarWriter(input.toDF(), withTopic = Some(topic))(
          withSelectExpr = s"'$topic' as __topic",
          "value as __key"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
  }

  test("streaming - write data with valid schema but wrong types") {
    val input = MemoryStream[String]
    val topic = newTopic()

    var writer: StreamingQuery = null
    var ex: Exception = null

    try {
      ex = intercept[StreamingQueryException] {
        /* key field wrong type */
        writer = createPulsarWriter(input.toDF(), withTopic = Some(topic))(
          withSelectExpr = s"'$topic' as topic",
          "CAST(value as INT) as __key",
          "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains("key attribute type must be a string or binary"))

    try {
      ex = intercept[StreamingQueryException] {
        /* eventTime field wrong type */
        writer = createPulsarWriter(input.toDF(), withTopic = Some(topic))(
          withSelectExpr = s"'$topic' as topic",
          "value as __eventTime",
          "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(
      ex.getMessage
        .toLowerCase(Locale.ROOT)
        .contains("__eventtime attribute type must be a bigint or timestamp"))
  }

  test("batch - write to pulsar with producer conf case sensitive") {
    val topic = newTopic()
    val df = Seq("1", "2", "3", "4", "5") map { v =>
      (v, v)
    } toDF ("key", "value")
    df.write
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)
      .option("pulsar.producer.blockIfQueueFull", "true")
      .option("pulsar.producer.maxPendingMessages", "100000")
      .option("pulsar.producer.sendTimeoutMs", "30000")
      .save()

    checkAnswer(
      createPulsarReader(topic).selectExpr("CAST(value as STRING) value"),
      Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }

  test("streaming - write to pulsar with producer case sensitive conf") {
    val input = MemoryStream[String]
    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = None,
      withOutputMode = Some(OutputMode.Append),
      withOptions = Map(
        "pulsar.producer.blockIfQueueFull" -> "true",
        "pulsar.producer.maxPendingMessages" -> "100000",
        "pulsar.producer.sendTimeoutMs" -> "30000")
    )(withSelectExpr = s"'$topic' as __topic", "value")

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(__key as STRING) __key", "CAST(value as STRING) value")
      .selectExpr("CAST(__key as INT) __key", "CAST(value as INT) value")
      .as[(Option[Int], Int)]
      .map(_._2)

    try {
      input.addData("1", "2", "3", "4", "5")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      input.addData("6", "7", "8", "9", "10")
      failAfter(streamingTimeout) {
        writer.processAllAvailable()
      }
      checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
    } finally {
      writer.stop()
    }
  }

  ignore("generic - write big data with small producer buffer") {
    /* This test ensures that we understand the semantics of Pulsar when
     * is comes to blocking on a call to send when the send buffer is full.
     * This test will configure the smallest possible producer buffer and
     * indicate that we should block when it is full. Thus, no exception should
     * be thrown in the case of a full buffer.
     */
    val topic = newTopic()
    val clientOptions = new java.util.HashMap[String, Object]
    clientOptions.put(SERVICE_URL_OPTION_KEY, serviceUrl)
    clientOptions.put("buffer.memory", "16384") // min buffer size
    clientOptions.put("block.on.buffer.full", "true")
    val producerOptions = new java.util.HashMap[String, Object]
    val inputSchema = Seq(AttributeReference("value", BinaryType)())
    val data = new Array[Byte](15000) // large value
    val writeTask =
      new PulsarWriteTask(clientOptions, producerOptions, Some(topic), inputSchema, "")
    try {
      val fieldTypes: Array[DataType] = Array(BinaryType)
      val converter = UnsafeProjection.create(fieldTypes)
      val row = new SpecificInternalRow(fieldTypes)
      row.update(0, data)
      val iter = Seq.fill(1000)(converter.apply(row)).iterator
      writeTask.execute(iter)
    } finally {
      writeTask.close()
    }
  }

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = TopicName.get(s"topic-${topicId.getAndIncrement()}").toString

  private def createPulsarReader(topic: String): DataFrame = {
    spark.read
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(ENDING_OFFSETS_OPTION_KEY, "latest")
      .option(TOPIC_SINGLE, topic)
      .load()
  }

  private def createPulsarWriter(
      input: DataFrame,
      withTopic: Option[String] = None,
      withOutputMode: Option[OutputMode] = None,
      withOptions: Map[String, String] = Map[String, String]())(
      withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      if (withSelectExpr.length > 0) {
        df = df.selectExpr(withSelectExpr: _*)
      }
      stream = df.writeStream
        .format("pulsar")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option(SERVICE_URL_OPTION_KEY, serviceUrl)
        .option(ADMIN_URL_OPTION_KEY, adminUrl)
        .queryName("pulsarStream")
      withTopic.foreach(stream.option(TOPIC_SINGLE, _))
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }
}
