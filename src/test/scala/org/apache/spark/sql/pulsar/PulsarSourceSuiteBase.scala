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

import org.apache.pulsar.client.admin.PulsarAdmin

import java.nio.charset.StandardCharsets.UTF_8
import java.sql.Date
import java.text.SimpleDateFormat
import java.util.Locale
import scala.reflect.ClassTag
import org.apache.pulsar.client.api.{MessageId, Schema}
import org.apache.pulsar.common.schema.SchemaInfo
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.pulsar.PulsarProvider.getPulsarOffset
import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{Encoder, Encoders, Row}
import org.apache.spark.util.Utils

abstract class PulsarSourceSuiteBase extends PulsarSourceTest {
  import PulsarOptions._
  import SchemaData._
  import testImplicits._

  test("cannot stop Pulsar stream") {
    val topic = newTopic()
    sendMessages(topic, (101 to 105).map { _.toString }.toArray)

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicPattern, s"$topic.*")

    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      StopStream
    )
  }

  for (failOnDataLoss <- Seq(true, false)) {
    test(s"assign from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromLatestOffsets(
        topic,
        addPartitions = false,
        failOnDataLoss = failOnDataLoss,
        TopicSingle -> topic)
    }

    test(s"assign from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromEarliestOffsets(
        topic,
        addPartitions = false,
        failOnDataLoss = failOnDataLoss,
        TopicSingle -> topic)
    }

    test(s"assign from time (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromTime(
        topic,
        addPartitions = false,
        failOnDataLoss = failOnDataLoss,
        TopicSingle -> topic)
    }

    test(s"assign from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromSpecificOffsets(
        topic,
        failOnDataLoss = failOnDataLoss,
        TopicSingle -> topic,
        FailOnDataLossOptionKey -> failOnDataLoss.toString)
    }

    test(s"subscribing topic by name from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromLatestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        TopicMulti -> topic)
    }

    test(s"subscribing topic by name from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromEarliestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        TopicMulti -> topic)
    }

    test(s"subscribing topic by name from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromSpecificOffsets(topic, failOnDataLoss = failOnDataLoss, TopicMulti -> topic)
    }

    test(s"subscribing topic by pattern from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromLatestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        TopicPattern -> s"$topicPrefix-.*")
    }

    test(s"subscribing topic by pattern from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromEarliestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        TopicPattern -> s"$topicPrefix-.*")
    }

    test(s"subscribing topic by pattern from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromSpecificOffsets(
        topic,
        failOnDataLoss = failOnDataLoss,
        TopicPattern -> s"$topicPrefix-.*")
    }
  }

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark.readStream
          .format("pulsar")
          .option(ServiceUrlOptionKey, serviceUrl)
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    // Specifying an ending offset
    testBadOptions(EndingOffsetsOptionKey -> "latest")(
      "Ending offset not valid in streaming queries")

    // No strategy specified
    testBadOptions()("one of the topic options", TopicSingle, TopicMulti, TopicPattern)

    // Multiple strategies specified
    testBadOptions(TopicMulti -> "t", TopicPattern -> "t.*")("one of the topic options")

    testBadOptions(TopicMulti -> "t", TopicSingle -> """{"a":[0]}""")(
      "one of the topic options")

    testBadOptions(TopicSingle -> "")("no topic is specified")
    testBadOptions(TopicMulti -> "")("No topics is specified")
    testBadOptions(TopicPattern -> "")("TopicsPattern is empty")
  }

  // TODO: figure out what the new behavior is and re-enable the test.
  ignore("get offsets from case insensitive parameters") {
    for ((optionKey, optionValue, answer) <- Seq(
      (StartingOffsetsOptionKey, "earLiEst", EarliestOffset),
      (EndingOffsetsOptionKey, "laTest", LatestOffset))) {
      val offset = getPulsarOffset(Map(optionKey -> optionValue), offsetOptionKey = optionKey, answer)
      assert(offset === answer)
    }

    for ((optionKey, answer) <- Seq(
      (StartingOffsetsOptionKey, EarliestOffset),
      (EndingOffsetsOptionKey, LatestOffset))) {
      val offset = getPulsarOffset(Map.empty, offsetOptionKey = optionKey, answer)
      assert(offset === answer)
    }
  }

  test("Pulsar column types") {
    val now = System.currentTimeMillis()
    val topic = newTopic()
    val mid = sendMessages(topic, Array(1).map(_.toString)).last._2

    val pulsar = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(StartingOffsetsOptionKey, "earliest")
      .option(TopicMulti, topic)
      .load()

    val query = pulsar.writeStream
      .format("memory")
      .queryName("pulsarColumnTypes")
      .trigger(defaultTrigger)
      .start()
    eventually(timeout(streamingTimeout)) {
      assert(
        spark.table("pulsarColumnTypes").count() == 1,
        s"Unexpected results: ${spark.table("pulsarColumnTypes").collectAsList()}")
    }
    val row = spark.table("pulsarColumnTypes").head()
    assert(row.getAs[Array[Byte]]("__key") === null, s"Unexpected results: $row")
    assert(row.getAs[Array[Byte]]("value") === "1".getBytes(UTF_8), s"Unexpected results: $row")
    assert(row.getAs[String]("__topic") === topic, s"Unexpected results: $row")
    assert(MessageId.fromByteArray(row.getAs[Array[Byte]]("__messageId")) === mid, s"Unexpected results: $row")
    // We cannot check the exact timestamp as it's the time that messages were inserted by the
    // producer. So here we just use a low bound to make sure the internal conversion works.
    assert(
      row.getAs[java.sql.Timestamp]("__publishTime").getTime >= now,
      s"Unexpected results: $row")
    assert(row.getAs[java.sql.Timestamp]("__eventTime") === null, s"Unexpected results: $row")
    query.stop()
  }

  private def check[T: ClassTag](
      schemaInfo: SchemaInfo,
      datas: Seq[T],
      encoder: Encoder[T],
      str: T => String) = {
    val topic = newTopic()
    createPulsarSchema(topic, schemaInfo)

    val tpe = schemaInfo.getType

    val reader = spark.readStream
      .format("pulsar")
      .option(StartingOffsetsOptionKey, "earliest")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, true)
      .option(TopicSingle, topic)

    if (str == null) {
      val pulsar = reader
        .load()
        .selectExpr("value")
        .as[T](encoder)

      testStream(pulsar)(
        AddPulsarTypedData(Set(topic), tpe, datas),
        CheckAnswer(datas: _*)(encoder)
      )
    } else {
      val pulsar = reader
        .load()
        .selectExpr("CAST(value as String)")
        .as[String]
      testStream(pulsar)(
        AddPulsarTypedData(Set(topic), tpe, datas),
        CheckAnswer(datas.map(str(_)): _*)
      )
    }
  }

  test("test boolean stream") {
    check[Boolean](Schema.BOOL.getSchemaInfo, booleanSeq, Encoders.scalaBoolean, null)
  }

  test("test int stream") {
    check[Int](Schema.INT32.getSchemaInfo, int32Seq, Encoders.scalaInt, null)
  }

  test("test string stream") {
    check[String](Schema.STRING.getSchemaInfo, stringSeq, Encoders.STRING, null)
  }

  test("test byte stream") {
    check[Byte](Schema.INT8.getSchemaInfo, int8Seq, Encoders.scalaByte, null)
  }

  test("test double stream") {
    check[Double](Schema.DOUBLE.getSchemaInfo, doubleSeq, Encoders.scalaDouble, null)
  }

  test("test float stream") {
    check[Float](Schema.FLOAT.getSchemaInfo, floatSeq, Encoders.scalaFloat, null)
  }

  test("test short stream") {
    check[Short](Schema.INT16.getSchemaInfo, int16Seq, Encoders.scalaShort, null)
  }

  test("test long stream") {
    check[Long](Schema.INT64.getSchemaInfo, int64Seq, Encoders.scalaLong, null)
  }

  test("test byte array stream") {
    check[Array[Byte]](Schema.BYTES.getSchemaInfo, bytesSeq, Encoders.BINARY, new String(_))
  }

  test("test date stream") {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    check[Date](
      Schema.DATE.getSchemaInfo,
      dateSeq,
      Encoders.DATE,
      dateFormat.format(_))
  }

  test("test timestamp stream") {
    val tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    check[java.sql.Timestamp](
      Schema.TIMESTAMP.getSchemaInfo,
      timestampSeq,
      Encoders.kryo(classOf[java.sql.Timestamp]),
      tsFormat.format(_))
  }

  test("test struct types in avro") {
    import SchemaData._

    val topic = newTopic()
    val si = Schema.AVRO(classOf[Foo]).getSchemaInfo
    createPulsarSchema(topic, si)
    val reader = spark.readStream
      .format("pulsar")
      .option(StartingOffsetsOptionKey, "earliest")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, true)
      .option(TopicSingle, topic)

    val pulsar = reader.load().selectExpr("i", "f", "bar")
    testStream(pulsar)(
      AddPulsarTypedData(Set(topic), si.getType, fooSeq),
      CheckAnswer(fooSeq: _*)
    )
  }

  test("test struct types in json") {
    import SchemaData._

    val topic = newTopic()
    val si = Schema.JSON(classOf[F1]).getSchemaInfo
    createPulsarSchema(topic, si)
    val reader = spark.readStream
      .format("pulsar")
      .option(StartingOffsetsOptionKey, "earliest")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, true)
      .option(TopicSingle, topic)

    val pulsar = reader.load().selectExpr("baz.f", "baz.d", "baz.mp", "baz.arr")

    testStream(pulsar)(
      AddPulsarTypedData(Set(topic), si.getType, f1Seq),
      CheckAnswer(f1Results: _*)
    )
  }

  def testDifferentMessageTypes(trigger: Trigger, sendFunc: (String, Int, Int) => Unit): Unit = {
    val inputTopic = newTopic()
    val outputTopic = newTopic()

    val queryName = "test"

    val numMessages = 10
    var query: StreamingQuery = null
    try {
      withTempPaths(1) {
        case Seq(checkpointDir) =>
          def startQuery(): StreamingQuery = {
            spark.readStream
              .format("pulsar")
              .option(StartingOffsetsOptionKey, "earliest")
              .option(ServiceUrlOptionKey, serviceUrl)
              .option(FailOnDataLossOptionKey, true)
              .option(TopicSingle, inputTopic)
              .load()
              .select(col("value").cast("STRING"))
              .writeStream
              .trigger(trigger)
              .queryName(queryName)
              .format("pulsar")
              .option(ServiceUrlOptionKey, serviceUrl)
              .option(TopicSingle, outputTopic)
              .option("checkpointLocation", checkpointDir.getCanonicalPath)
              .start()
          }

          if (trigger.getClass.getName.contains("AvailableNowTrigger")) {
            // ===== TEST OUTPUT FOR o.a.s.sql.pulsar.PulsarMicroBatchV1SourceSuite: 'test with batched and non-batched messages with trigger: AvailableNowTrigger' =====
            logInfo("Starting query with AvailableNowTrigger")
            for (i <- 0 until 10) {
              sendFunc(inputTopic, i, numMessages)
              query = startQuery()
              query.processAllAvailable()
              logInfo("!--- done with processAllAvailable")
              query.awaitTermination()
              logInfo("!--- done with awaitTermination")
              // results will be cumulative

              checkAnswer(
                spark.read.format("pulsar")
                  .option(ServiceUrlOptionKey, serviceUrl)
                  .option(TopicSingle, outputTopic)
                  .load().select(col("value").cast("STRING")),
                (0 until (i + 1) * numMessages).map(r => Row(r.toString))
              )
              logInfo(s"====================== FINISHED ITERATION $i ======================")
            }
          } else {
            query = startQuery()
            for (i <- 0 until 10) {
              sendFunc(inputTopic, i, numMessages)
              query.processAllAvailable()
              // results will be cumulative
              checkAnswer(spark.read.format("pulsar")
                .option(ServiceUrlOptionKey, serviceUrl)
                .option(TopicSingle, outputTopic)
                .load().select(col("value").cast("STRING")),
                (0 until (i + 1) * numMessages).map(r => Row(r.toString)))
              logInfo(s"====================== FINISHED ITERATION $i ======================")
            }
          }
      }
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }

  List(Trigger.ProcessingTime(0), Trigger.AvailableNow()).foreach { trigger =>
    test("test with batched messages with trigger: " + trigger) {
      testDifferentMessageTypes(trigger, (topic, i, numMessages) => {
        sendMessages(topic, (i * numMessages until (i + 1) * numMessages).map(_.toString).toArray, None, batched = true)
      })
    }

    test("test with non-batched messages with trigger: " + trigger) {
      testDifferentMessageTypes(trigger, (topic, i, numMessages) => {
        sendMessages(topic, (i * numMessages until (i + 1) * numMessages).map(_.toString).toArray, None, batched = false)
      })
    }

    test("test with batched and non-batched messages with trigger: " + trigger) {
      testDifferentMessageTypes(trigger, (topic, i, numMessages) => {
        var sent = 0
        while (sent < numMessages) {
          if (sent % 3 == 0) {
            val messagesToSend = if (numMessages - sent < 2) 1 else 2
            sendMessages(topic, (i * numMessages + sent until i * numMessages + sent + messagesToSend).map(_.toString).toArray, None, batched = true)
            sent += 2
          } else {
            sendMessages(topic, (i * numMessages + sent until i * numMessages + sent + 1).map(_.toString).toArray, None, batched = false)
            sent += 1
          }
        }
      })
    }
  }

  private def testFromLatestOffsets(
      topic: String,
      addPartitions: Boolean,
      failOnDataLoss: Boolean,
      options: (String, String)*): Unit = {

    sendMessages(topic, Array("-1"))
    require(getLatestOffsets(Set(topic)).size === 1)

    val reader = spark.readStream
      .format("pulsar")
      .option(StartingOffsetsOptionKey, "latest")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, failOnDataLoss.toString)

    options.foreach { case (k, v) => reader.option(k, v) }
    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4), // Should get the data back on recovery
      StopStream,
      AddPulsarData(Set(topic), 4, 5, 6), // Add data when stream is stopped
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7), // Should get the added data
      AddPulsarData(Set(topic), 7, 8),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        true
      },
      AddPulsarData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromEarliestOffsets(
      topic: String,
      addPartitions: Boolean,
      failOnDataLoss: Boolean,
      options: (String, String)*): Unit = {

    sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(getLatestOffsets(Set(topic)).size === 1)

    val reader = spark.readStream
    reader
      .format("pulsar")
      .option(StartingOffsetsOptionKey, "earliest")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, failOnDataLoss.toString)
    options.foreach { case (k, v) => reader.option(k, v) }
    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      AddPulsarData(Set(topic), 4, 5, 6), // Add data when stream is stopped
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      StopStream,
      AddPulsarData(Set(topic), 7, 8),
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AssertOnQuery("Add partitions") { query: StreamExecution =>
        true
      },
      AddPulsarData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromTime(
      topic: String,
      addPartitions: Boolean,
      failOnDataLoss: Boolean,
      options: (String, String)*): Unit = {

    val time0 = System.currentTimeMillis() - 10000

    sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(getLatestOffsets(Set(topic)).size === 1)

    def dfAfter(ts: Long) = {
      val reader = spark.readStream
      reader
        .format("pulsar")
        .option(StartingTime, time0)
        .option(ServiceUrlOptionKey, serviceUrl)
        .option(FailOnDataLossOptionKey, failOnDataLoss.toString)
      options.foreach { case (k, v) => reader.option(k, v) }
      val pulsar = reader
        .load()
        .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
      val mapped = pulsar.map(kv => kv._2.toInt + 1)
      mapped
    }

    testStream(dfAfter(time0))(
      AddPulsarData(Set(topic), 7, 8, 9),
      CheckAnswer(2, 3, 4, 8, 9, 10)
    )
  }

  private def testFromSpecificOffsets(
      topic: String,
      failOnDataLoss: Boolean,
      options: (String, String)*): Unit = {

    val mids = sendMessages(
      topic,
      Array(
        //  0,   1,   2,  3, 4, 5,  6, 7,  8
        -20, -21, -22, 1, 2, 3, 10, 11, 12).map(_.toString),
      None).map(_._2)

    val s1 = JsonUtils.topicOffsets(Map(topic -> mids(3)))

    val reader = spark.readStream
      .format("pulsar")
      .option(StartingOffsetsOptionKey, s1)
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, failOnDataLoss.toString)
    options.foreach { case (k, v) => reader.option(k, v) }
    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt)

    testStream(mapped)(
      AddPulsarData(Set(topic), 7),
      CheckAnswer(1, 2, 3, 10, 11, 12, 7),
      StopStream,
      StartStream(),
      CheckAnswer(1, 2, 3, 10, 11, 12, 7), // Should get the data back on recovery
      StopStream,
      StartStream(),
      AddPulsarData(Set(topic), 30, 31, 32, 33, 34),
      CheckAnswer(1, 2, 3, 10, 11, 12, 7, 30, 31, 32, 33, 34)
    )
  }
}
