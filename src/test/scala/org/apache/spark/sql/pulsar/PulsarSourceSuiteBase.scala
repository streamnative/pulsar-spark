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

import java.nio.charset.StandardCharsets.UTF_8
import java.text.SimpleDateFormat
import java.util.{Date, Locale}

import scala.reflect.ClassTag
import org.apache.pulsar.client.api.{MessageId, Schema}
import org.apache.pulsar.common.schema.SchemaInfo
import org.apache.spark.sql.execution.streaming.StreamExecution
import org.apache.spark.sql.{Encoder, Encoders}

abstract class PulsarSourceSuiteBase extends PulsarSourceTest {
  import PulsarOptions._
  import PulsarProvider._
  import SchemaData._
  import testImplicits._

  test("cannot stop Pulsar stream") {
    val topic = newTopic()
    sendMessages(topic, (101 to 105).map { _.toString }.toArray)

    val reader = spark.readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_PATTERN, s"$topic.*")

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
        TOPIC_SINGLE -> topic)
    }

    test(s"assign from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromEarliestOffsets(
        topic,
        addPartitions = false,
        failOnDataLoss = failOnDataLoss,
        TOPIC_SINGLE -> topic)
    }

    test(s"assign from time (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromTime(
        topic,
        addPartitions = false,
        failOnDataLoss = failOnDataLoss,
        TOPIC_SINGLE -> topic)
    }

    test(s"assign from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromSpecificOffsets(
        topic,
        failOnDataLoss = failOnDataLoss,
        TOPIC_SINGLE -> topic,
        FAIL_ON_DATA_LOSS_OPTION_KEY -> failOnDataLoss.toString)
    }

    test(s"subscribing topic by name from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromLatestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        TOPIC_MULTI -> topic)
    }

    test(s"subscribing topic by name from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromEarliestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        TOPIC_MULTI -> topic)
    }

    test(s"subscribing topic by name from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topic = newTopic()
      testFromSpecificOffsets(topic, failOnDataLoss = failOnDataLoss, TOPIC_MULTI -> topic)
    }

    test(s"subscribing topic by pattern from latest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromLatestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        TOPIC_PATTERN -> s"$topicPrefix-.*")
    }

    test(s"subscribing topic by pattern from earliest offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromEarliestOffsets(
        topic,
        addPartitions = true,
        failOnDataLoss = failOnDataLoss,
        TOPIC_PATTERN -> s"$topicPrefix-.*")
    }

    test(s"subscribing topic by pattern from specific offsets (failOnDataLoss: $failOnDataLoss)") {
      val topicPrefix = newTopic()
      val topic = topicPrefix + "-suffix"
      testFromSpecificOffsets(
        topic,
        failOnDataLoss = failOnDataLoss,
        TOPIC_PATTERN -> s"$topicPrefix-.*")
    }
  }

  test("bad source options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark.readStream
          .format("pulsar")
          .option(SERVICE_URL_OPTION_KEY, serviceUrl)
          .option(ADMIN_URL_OPTION_KEY, adminUrl)
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    // Specifying an ending offset
    testBadOptions(ENDING_OFFSETS_OPTION_KEY -> "latest")(
      "Ending offset not valid in streaming queries")

    // No strategy specified
    testBadOptions()("one of the topic options", TOPIC_SINGLE, TOPIC_MULTI, TOPIC_PATTERN)

    // Multiple strategies specified
    testBadOptions(TOPIC_MULTI -> "t", TOPIC_PATTERN -> "t.*")("one of the topic options")

    testBadOptions(TOPIC_MULTI -> "t", TOPIC_SINGLE -> """{"a":[0]}""")(
      "one of the topic options")

    testBadOptions(TOPIC_SINGLE -> "")("no topic is specified")
    testBadOptions(TOPIC_MULTI -> "")("No topics is specified")
    testBadOptions(TOPIC_PATTERN -> "")("TopicsPattern is empty")
  }

  test("get offsets from case insensitive parameters") {
    for ((optionKey, optionValue, answer) <- Seq(
      (STARTING_OFFSETS_OPTION_KEY, "earLiEst", EarliestOffset),
      (ENDING_OFFSETS_OPTION_KEY, "laTest", LatestOffset))) {
      val offset = getPulsarOffset(Map(optionKey -> optionValue), optionKey, answer)
      assert(offset === answer)
    }

    for ((optionKey, answer) <- Seq(
      (STARTING_OFFSETS_OPTION_KEY, EarliestOffset),
      (ENDING_OFFSETS_OPTION_KEY, LatestOffset))) {
      val offset = getPulsarOffset(Map.empty, optionKey, answer)
      assert(offset === answer)
    }
  }

  test("Pulsar column types") {
    val now = System.currentTimeMillis()
    val topic = newTopic()
    val mid = sendMessages(topic, Array(1).map(_.toString))(0)._2

    val pulsar = spark.readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(TOPIC_MULTI, topic)
      .load()

    val query = pulsar.writeStream
      .format("memory")
      .queryName("pulsarColumnTypes")
      .trigger(defaultTrigger)
      .start()
    eventually(timeout(streamingTimeout)) {
      assert(
        spark.table("pulsarColumnTypes").count == 1,
        s"Unexpected results: ${spark.table("pulsarColumnTypes").collectAsList()}")
    }
    val row = spark.table("pulsarColumnTypes").head()
    assert(row.getAs[Array[Byte]]("__key") === null, s"Unexpected results: $row")
    assert(row.getAs[Array[Byte]]("value") === "1".getBytes(UTF_8), s"Unexpected results: $row")
    assert(row.getAs[String]("__topic") === topic, s"Unexpected results: $row")
    val rowAsMid = MessageId.fromByteArray(row.getAs[Array[Byte]]("__messageId"))
    assert(PulsarSourceUtils.messageIdRoughEquals(rowAsMid, mid), s"Unexpected results: $rowAsMid")
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
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(FAIL_ON_DATA_LOSS_OPTION_KEY, true)
      .option(TOPIC_SINGLE, topic)

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
      Encoders.bean(classOf[Date]),
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
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(FAIL_ON_DATA_LOSS_OPTION_KEY, true)
      .option(TOPIC_SINGLE, topic)

    val pulsar = reader.load().selectExpr("i", "f", "bar")
    testStream(pulsar)(
      AddPulsarTypedData(Set(topic), si.getType, fooSeq),
      CheckAnswer(fooSeq: _*)
    )
  }

  test("test struct types with multiple avro version ") {
    import SchemaData._

    val topic = newTopic()
    val si = Schema.AVRO(classOf[Foo]).getSchemaInfo
    createPulsarSchema(topic, si)
    val si2 = Schema.AVRO(classOf[FooV2]).getSchemaInfo
    createPulsarSchema(topic, si2)

    val fooSeqV2AsV1 = fooSeqV2.map(fooV2 => Foo(fooV2.i, fooV2.f, null))
    val fooSeqV1AsV2 = fooSeq.map(foo => FooV2(foo.i, foo.f))
    val fooSeqV1All = fooSeq ++ fooSeqV2AsV1

    val reader = spark.readStream
      .format("pulsar")
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(FAIL_ON_DATA_LOSS_OPTION_KEY, true)
      .option(TOPIC_SINGLE, topic)

    val pulsar = reader.option(TOPIC_VERSION, "0").load().selectExpr("i", "f", "bar")

    testStream(pulsar)(
      AddPulsarTypedData(Set(topic), si.getType, fooSeq),
      AddPulsarTypedData(Set(topic), si.getType, fooSeqV2),
      CheckAnswer(fooSeqV1All: _*)
    )

    val pulsarV2 = reader.load().selectExpr("i", "f")
    testStream(pulsarV2)(
      AddPulsarTypedData(Set(topic), si.getType, fooSeqV2),
      CheckAnswer(fooSeqV1AsV2 ++ fooSeqV2 ++ fooSeqV2 : _*)
    )
  }


  test("test struct types in json") {
    import SchemaData._

    val topic = newTopic()
    val si = Schema.JSON(classOf[F1]).getSchemaInfo
    createPulsarSchema(topic, si)
    val reader = spark.readStream
      .format("pulsar")
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(FAIL_ON_DATA_LOSS_OPTION_KEY, true)
      .option(TOPIC_SINGLE, topic)

    val pulsar = reader.load().selectExpr("baz.f", "baz.d", "baz.mp", "baz.arr")

    testStream(pulsar)(
      AddPulsarTypedData(Set(topic), si.getType, f1Seq),
      CheckAnswer(f1Results: _*)
    )
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
      .option(STARTING_OFFSETS_OPTION_KEY, "latest")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(FAIL_ON_DATA_LOSS_OPTION_KEY, failOnDataLoss.toString)

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
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(FAIL_ON_DATA_LOSS_OPTION_KEY, failOnDataLoss.toString)
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

    val time0 = System.currentTimeMillis()
    Thread.sleep(5000)
    sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(getLatestOffsets(Set(topic)).size === 1)

    def dfAfter(ts: Long) = {
      val reader = spark.readStream
      reader
        .format("pulsar")
        .option(STARTING_TIME, time0)
        .option(SERVICE_URL_OPTION_KEY, serviceUrl)
        .option(ADMIN_URL_OPTION_KEY, adminUrl)
        .option(FAIL_ON_DATA_LOSS_OPTION_KEY, failOnDataLoss.toString)
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
      .option(STARTING_OFFSETS_OPTION_KEY, s1)
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(FAIL_ON_DATA_LOSS_OPTION_KEY, failOnDataLoss.toString)
    options.foreach { case (k, v) => reader.option(k, v) }
    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt)

    testStream(mapped)(
      makeSureGetOffsetCalled,
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

