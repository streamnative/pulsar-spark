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

import org.scalatest.time.SpanSugar._

import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.catalyst.expressions.{AttributeReference, SpecificInternalRow, UnsafeProjection}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, Trigger}
import org.apache.spark.sql.types.{BinaryType, DataType}
import org.apache.spark.util.Utils

class PulsarContinuousSinkSuite extends PulsarContinuousTest {

  import testImplicits._
  import PulsarOptions._

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
      withSelectExpr = s"'$topic' as topic", "value")

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
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
      withOutputMode = Some(OutputMode.Append()))()

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(key as STRING) key", "CAST(value as STRING) value")
      .selectExpr("CAST(key as INT) key", "CAST(value as INT) value")
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
      withSelectExpr = "'foo' as topic", "CAST(value as STRING) value")

    val reader = createPulsarReader(topic)
      .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
      .selectExpr("CAST(key AS INT)", "CAST(value AS INT)")
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
        withSelectExpr = "CAST(null as STRING) as topic", "value"
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
        withSelectExpr = "value as key", "value"
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
      .contains("topic option required when no 'topic' attribute is present"))

    try {
      /* No value field */
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = s"'$topic' as topic", "value as key"
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
      "required attribute 'value' not found"))
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
        withSelectExpr = s"CAST('1' as INT) as topic", "value"
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
      /* value field wrong type */
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = s"'$topic' as topic", "CAST(value as INT) as value"
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
      "value attribute type must be a string or binary"))

    try {
      /* key field wrong type */
      writer = createPulsarWriter(input.toDF())(
        withSelectExpr = s"'$topic' as topic", "CAST(value as INT) as key", "value"
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
    val writeTask = new PulsarStreamDataWriter(inputSchema, null, null, Some(topic))
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
