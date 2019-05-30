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

import java.util.Locale
import java.util.concurrent.atomic.AtomicInteger

import org.apache.pulsar.common.naming.TopicName
import org.scalatest.time.SpanSugar._
import org.apache.spark.SparkException
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{AnalysisException, DataFrame, Row, SaveMode}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamTest, StreamingQuery, StreamingQueryException}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{BinaryType, DataType}

class PulsarSinkSuite extends StreamTest with SharedSQLContext with PulsarTest {
  import testImplicits._
  import PulsarOptions._

  override val streamingTimeout = 30.seconds

  test("batch - write to pulsar") {
    val topic = newTopic()
    val df = Seq("1", "2", "3", "4", "5") map { v =>
      (v, v)
    } toDF("key", "value")
    df.write
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(TOPIC_SINGLE, topic)
      .save()

    checkAnswer(
      createPulsarReader(topic).selectExpr("CAST(value as STRING) value"),
      Row("1") :: Row("2") :: Row("3") :: Row("4") :: Row("5") :: Nil)
  }

  test("batch - null topic field value, and no topic option") {
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")
    val ex = intercept[SparkException] {
      df.write
        .format("pulsar")
        .option(SERVICE_URL_OPTION_KEY, serviceUrl)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "null topic present in the data"))
  }

  test("batch - unsupported save modes") {
    val topic = newTopic()
    val df = Seq[(String, String)](null.asInstanceOf[String] -> "1").toDF("topic", "value")

    // Test bad save mode Ignore
    var ex = intercept[AnalysisException] {
      df.write
        .format("pulsar")
        .option(SERVICE_URL_OPTION_KEY,serviceUrl)
        .mode(SaveMode.Ignore)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      s"save mode ignore not allowed for pulsar"))

    // Test bad save mode Overwrite
    ex = intercept[AnalysisException] {
      df.write
        .format("pulsar")
        .option(SERVICE_URL_OPTION_KEY, serviceUrl)
        .mode(SaveMode.Overwrite)
        .save()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      s"save mode overwrite not allowed for pulsar"))
  }

  test("streaming - write to pulsar with topic field") {
    val input = MemoryStream[String]
    val topic = newTopic()

    val writer = createPulsarWriter(
      input.toDF(),
      withTopic = None,
      withOutputMode = Some(OutputMode.Append))(
      withSelectExpr = s"'$topic' as topic", "value")

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
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
      withSelectExpr = "CAST(value as STRING) key", "CAST(count as STRING) value"
    )

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
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
      withSelectExpr = "'foo' as topic",
      "CAST(value as STRING) key", "CAST(count as STRING) value")

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(key AS INT)", "CAST(value AS INT)")
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

  test("streaming - write data with bad schema") {
    val input = MemoryStream[String]
    val topic = newTopic()

    var writer: StreamingQuery = null
    var ex: Exception = null

    try {
      /* No value field */
      ex = intercept[StreamingQueryException] {
        writer = createPulsarWriter(input.toDF(), withTopic = Some(topic))(
          withSelectExpr = s"'$topic' as topic", "value as key"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "required attribute 'value' not found"))
  }

  test("streaming - write data with valid schema but wrong types") {
    val input = MemoryStream[String]
    val topic = newTopic()

    var writer: StreamingQuery = null
    var ex: Exception = null

    try {
      /* value field wrong type */
      ex = intercept[StreamingQueryException] {
        writer = createPulsarWriter(input.toDF(), withTopic = Some(topic))(
          withSelectExpr = s"'$topic' as topic", "CAST(value as INT) as value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "value attribute type must be a string or binary"))

    try {
      ex = intercept[StreamingQueryException] {
        /* key field wrong type */
        writer = createPulsarWriter(input.toDF(), withTopic = Some(topic))(
          withSelectExpr = s"'$topic' as topic", "CAST(value as INT) as key", "value"
        )
        input.addData("1", "2", "3", "4", "5")
        writer.processAllAvailable()
      }
    } finally {
      writer.stop()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(
      "key attribute type must be a string or binary"))
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
    val writeTask = new PulsarWriteTask(clientOptions, producerOptions, Some(topic), inputSchema)
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
      withOptions: Map[String, String] = Map[String, String]())
      (withSelectExpr: String*): StreamingQuery = {
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
        .queryName("pulsarStream")
      withTopic.foreach(stream.option(TOPIC_SINGLE, _))
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }
}
