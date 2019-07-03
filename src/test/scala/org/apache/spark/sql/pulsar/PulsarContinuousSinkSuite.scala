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
import java.util.{Date, Locale}

import scala.reflect.ClassTag

import org.scalatest.time.SpanSugar._
import org.apache.pulsar.client.api.Schema
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

import org.apache.spark.sql.{DataFrame, Encoder, Encoders, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.util.Utils

class PulsarContinuousSinkSuite extends PulsarContinuousTest {

  import testImplicits._
  import PulsarOptions._
  import SchemaData._

  override val streamingTimeout = 30.seconds

  test("streaming - write to pulsar with topic field") {
    val inputTopic = newTopic()

    val input = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, inputTopic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()

    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"'$topic' as __topic", "value")

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(__key as STRING) __key", "CAST(value as STRING) value")
      .selectExpr("CAST(__key as INT) __key", "CAST(value as INT) value")
      .as[(Option[Int], Int)]
      .map(_._2)

    try {
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      eventually(timeout(streamingTimeout)) {
        checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      }
      sendMessages(inputTopic, Array("6", "7", "8", "9", "10"))
      eventually(timeout(streamingTimeout)) {
        checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      }
    } finally {
      writer.stop()
    }
  }

  test("streaming - write w/o topic field, with topic option") {
    val inputTopic = newTopic()

    val input = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, inputTopic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()

    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append))()

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(__key as STRING) __key", "CAST(value as STRING) value")
      .selectExpr("CAST(__key as INT) __key", "CAST(value as INT) value")
      .as[(Option[Int], Int)]
      .map(_._2)

    try {
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      eventually(timeout(streamingTimeout)) {
        checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      }
      sendMessages(inputTopic, Array("6", "7", "8", "9", "10"))
      eventually(timeout(streamingTimeout)) {
        checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      }
    } finally {
      writer.stop()
    }
  }

  test("streaming - topic field and topic option") {
    /* The purpose of this test is to ensure that the topic option
     * overrides the topic field. We begin by writing some data that
     * includes a topic field and value (e.g., 'foo') along with a topic
     * option. Then when we read from the topic specified in the option
     * we should see the data i.e., the data was written to the topic
     * option, and not to the topic in the data e.g., foo
     */
    val inputTopic = newTopic()

    val input = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, inputTopic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()

    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append()))(
      withSelectExpr = "'foo' as __topic", "CAST(value as STRING) value")

    try {
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      Thread.sleep(2000)

      val reader = createPulsarReader(topic)
        .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
        .selectExpr("CAST(__key AS INT)", "CAST(value AS INT)")
        .as[(Option[Int], Int)]
        .map(_._2)

      eventually(timeout(streamingTimeout)) {
        checkDatasetUnorderly(reader, 1, 2, 3, 4, 5)
      }
      sendMessages(inputTopic, Array("6", "7", "8", "9", "10"))
      eventually(timeout(streamingTimeout)) {
        checkDatasetUnorderly(reader, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
      }
    } finally {
      writer.stop()
    }
  }

  private def check[T: ClassTag](schemaInfo: SchemaInfo, data: Seq[T], encoder: Encoder[T], str: T => String) = {
    val inputTopic = newTopic()
    val topic = newTopic()
    createPulsarSchema(inputTopic, schemaInfo)
    createPulsarSchema(topic, schemaInfo)

    val input = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, inputTopic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"'$topic' as __topic", "value")

    try {
      sendTypedMessages[T](inputTopic, schemaInfo.getType, data, None)
      Thread.sleep(2000)

      val reader = createPulsarReader(topic)
        .selectExpr("CAST(value AS STRING)")
        .as[String]

      eventually(timeout(streamingTimeout)) {
        if (str == null) {
          checkDatasetUnorderly[String](reader, data.map(_.toString) : _*)
        } else {
          checkDatasetUnorderly[String](reader, data.map(str(_)): _*)
        }
      }
    } finally {
      writer.stop()
    }
  }

  test("streaming - boolean") {
    check[Boolean](Schema.BOOL.getSchemaInfo, booleanSeq, Encoders.scalaBoolean, null)
  }

  test("streaming - int") {
    check[Int](Schema.INT32.getSchemaInfo, int32Seq, Encoders.scalaInt, null)
  }

  test("streaming - string") {
    check[String](Schema.STRING.getSchemaInfo, stringSeq, Encoders.STRING, null)
  }

  test("streaming - byte") {
    check[Byte](Schema.INT8.getSchemaInfo, int8Seq, Encoders.scalaByte, null)
  }

  test("streaming - double") {
    check[Double](Schema.DOUBLE.getSchemaInfo, doubleSeq, Encoders.scalaDouble, null)
  }

  test("streaming - float") {
    check[Float](Schema.FLOAT.getSchemaInfo, floatSeq, Encoders.scalaFloat, null)
  }

  test("streaming - short") {
    check[Short](Schema.INT16.getSchemaInfo, int16Seq, Encoders.scalaShort, null)
  }

  test("streaming - long") {
    check[Long](Schema.INT64.getSchemaInfo, int64Seq, Encoders.scalaLong, null)
  }

  test("streaming - byte array") {
    check[Array[Byte]](Schema.BYTES.getSchemaInfo, bytesSeq,
      Encoders.BINARY, new String(_))
  }

  test("streaming - date") {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    check[Date](Schema.DATE.getSchemaInfo, dateSeq,
      Encoders.bean(classOf[Date]), dateFormat.format(_))
  }

  test("streaming - timestamp") {
    val tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    check[java.sql.Timestamp](Schema.TIMESTAMP.getSchemaInfo, timestampSeq,
      Encoders.kryo(classOf[java.sql.Timestamp]), tsFormat.format(_))
  }

  test("streaming - struct type") {

    val schemaInfo = Schema.AVRO(classOf[Foo]).getSchemaInfo

    val inputTopic = newTopic()
    val topic = newTopic()
    createPulsarSchema(inputTopic, schemaInfo)

    val input = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, inputTopic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = "i", "f", "bar")

    try {
      sendTypedMessages[Foo](inputTopic, SchemaType.AVRO, fooSeq, None)
      Thread.sleep(2000)

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

      eventually(timeout(streamingTimeout)) {
        checkDatasetUnorderly[Foo](
          reader,
          fooSeq: _*)
      }
    } finally {
      writer.stop()
    }
  }

  test("null topic attribute") {
    val inputTopic = newTopic()

    val input = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, inputTopic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()
    val topic = newTopic()

    /* No topic field or topic option */
    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = "CAST(null as STRING) as __topic", "value"
      )
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      eventually(timeout(streamingTimeout)) {
        assert(writer.exception.isDefined)
        ex = writer.exception.get
      }
    } finally {
      writer.stop()
    }
    assert(ex.getCause.getCause.getMessage
      .toLowerCase(Locale.ROOT)
      .contains("null topic present in the data."))
  }

  test("streaming - write data with bad schema") {
    val inputTopic = newTopic()

    val input = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, inputTopic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()
    val topic = newTopic()

    /* No topic field or topic option */
    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = "value as __key", "value"
      )
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      eventually(timeout(streamingTimeout)) {
        assert(writer.exception.isDefined)
        ex = writer.exception.get
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage
      .toLowerCase(Locale.ROOT)
      .contains("topic option required when no '__topic' attribute is present"))

    try {
      /* No value field */
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = s"'$topic' as __topic", "value as __key"
      )
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      eventually(timeout(streamingTimeout)) {
        assert(writer.exception.isDefined)
        ex = writer.exception.get
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "schema should have at least one"))
  }

  test("streaming - write data with valid schema but wrong types") {
    val inputTopic = newTopic()

    val input = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, inputTopic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .load()
      .selectExpr("CAST(value as STRING) value")
    val topic = newTopic()

    var writer: StreamingQuery = null
    var ex: Exception = null
    try {
      /* topic field wrong type */
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = s"CAST('1' as INT) as __topic", "value"
      )
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      eventually(timeout(streamingTimeout)) {
        assert(writer.exception.isDefined)
        ex = writer.exception.get
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains("topic type must be a string"))

    try {
      /* key field wrong type */
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = s"'$topic' as __topic", "CAST(value as INT) as __key", "value"
      )
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      eventually(timeout(streamingTimeout)) {
        assert(writer.exception.isDefined)
        ex = writer.exception.get
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "key attribute type must be a string or binary"))

    try {
      /* eventTime field wrong type */
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = s"'$topic' as __topic", "value as __eventTime", "value"
      )
      sendMessages(inputTopic, Array("1", "2", "3", "4", "5"))
      eventually(timeout(streamingTimeout)) {
        assert(writer.exception.isDefined)
        ex = writer.exception.get
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "__eventtime attribute type must be a bigint or timestamp"))
  }

  ignore("generic - write big data with small producer buffer") {
    /* This test ensures that we understand the semantics of Pulsar when
    * is comes to blocking on a call to send when the send buffer is full.
    * This test will configure the smallest possible producer buffer and
    * indicate that we should block when it is full. Thus, no exception should
    * be thrown in the case of a full buffer.
    */
    val topic = newTopic()
    val options = new java.util.HashMap[String, String]
    options.put(SERVICE_URL_OPTION_KEY, serviceUrl)
    options.put("buffer.memory", "16384") // min buffer size
    options.put("block.on.buffer.full", "true")
    val inputSchema = Seq(AttributeReference("value", BinaryType)())
    val data = new Array[Byte](15000) // large value
    val writeTask = new PulsarStreamDataWriter(inputSchema, null, null, Some(topic), null)
    try {
      val fieldTypes: Array[DataType] = Array(BinaryType)
      val converter = UnsafeProjection.create(fieldTypes)
      val row = new SpecificInternalRow(fieldTypes)
      row.update(0, data)
      val iter = Seq.fill(1000)(converter.apply(row)).iterator
      iter.foreach(writeTask.write(_))
      writeTask.commit()
    } finally {
      writeTask.close()
    }
  }

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
      withOptions: Map[String, String] = Map[String, String]())
      (withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    val checkpointDir = Utils.createTempDir()
    var df = input.toDF()
    if (withSelectExpr.length > 0) {
      df = df.selectExpr(withSelectExpr: _*)
    }
    stream = df.writeStream
      .format("pulsar")
      .option("checkpointLocation", checkpointDir.getCanonicalPath)
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .trigger(Trigger.Continuous(1000))
      .queryName("pulsarStream")
    withTopic.foreach(stream.option("topic", _))
    withOutputMode.foreach(stream.outputMode(_))
    withOptions.foreach(opt => stream.option(opt._1, opt._2))
    stream.start()
  }
}
