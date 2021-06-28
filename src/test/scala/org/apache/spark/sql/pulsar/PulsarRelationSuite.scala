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
import java.time.{Clock, Instant, ZoneId}
import java.util.concurrent.atomic.AtomicInteger
import java.util.{Date, Locale}
import scala.reflect.ClassTag
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaType
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, Encoder, Encoders, QueryTest}

class PulsarRelationSuite extends QueryTest with SharedSparkSession with PulsarTest {
  import PulsarOptions._
  import SchemaData._
  import testImplicits._

  /*  override protected def sparkConf: SparkConf =
      super
        .sparkConf
        .set(SQLConf.USE_V1_SOURCE_LIST, "pulsar")*/

  private val topicId = new AtomicInteger(0)
  private def newTopic(): String = TopicName.get(s"topic-${topicId.getAndIncrement()}").toString

  private def createDF(
                        topic: String,
                        withOptions: Map[String, String] = Map.empty[String, String],
                        brokerAddress: Option[String] = None) = {
    val df = spark.read
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_MULTI, topic)
    withOptions.foreach {
      case (key, value) => df.option(key, value)
    }
    df.load().selectExpr("CAST(value AS STRING)")
  }

  test("explicit earliest to latest offsets") {
    val topic = newTopic()
    createTopic(topic, partitions = 3)
    sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    sendMessages(topic, Array("20"), Some(2))

    // Specify explicit earliest and latest offset values
    val df = createDF(
      topic,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df, (0 to 20).map(_.toString).toDF)

    // "latest" should late bind to the current (latest) offset in the df
    sendMessages(topic, (21 to 29).map(_.toString).toArray, Some(2))
    checkAnswer(df, (0 to 29).map(_.toString).toDF)
  }

  test("explicit starting time") {
    val topic = newTopic()
    createTopic(topic, partitions = 3)

    class TimeSettableClock extends Clock {

      var currentMillis: Long = 0L

      def setCurrentMillis(c: Long) = currentMillis = c

      override def getZone: ZoneId = ZoneId.systemDefault()

      override def withZone(zone: ZoneId): Clock = this

      override def instant(): Instant = Instant.ofEpochMilli(millis())

      override def millis(): Long = currentMillis
    }

    val settableClock = new TimeSettableClock()
    val t0 = System.currentTimeMillis()
    settableClock.setCurrentMillis(1)

    sendMessagesWithClock(topic, (0 to 9).map(_.toString).toArray, Some(0), settableClock)
    sendMessagesWithClock(topic, (10 to 19).map(_.toString).toArray, Some(1), settableClock)
    sendMessagesWithClock(topic, Array("20"), Some(2), settableClock)

    settableClock.setCurrentMillis(5)
    sendMessagesWithClock(topic, (21 to 25).map(_.toString).toArray, Some(2), settableClock)

    val df = createDF(
      topic,
      withOptions = Map("startingTime" -> "10", "polltimeoutms" -> "5000"))

    settableClock.setCurrentMillis(20)
    sendMessagesWithClock(topic, (26 to 29).map(_.toString).toArray, Some(2), settableClock)
    checkAnswer(df, (26 to 29).map(_.toString).toDF)
  }

  test("default starting and ending offsets") {
    val topic = newTopic()
    createTopic(topic, partitions = 3)
    sendMessages(topic, (0 to 9).map(_.toString).toArray, Some(0))
    sendMessages(topic, (10 to 19).map(_.toString).toArray, Some(1))
    sendMessages(topic, Array("20"), Some(2))

    // Implicit offset values, should default to earliest and latest
    val df = createDF(topic)
    // Test that we default to "earliest" and "latest"
    checkAnswer(df, (0 to 20).map(_.toString).toDF)
  }

  test("explicit offsets") {
    val topic1 = newTopic()
    val topic2 = newTopic()

    sendMessages(topic1, (0 to 9).map(_.toString).toArray, None)
    val t2mid: Seq[(String, MessageId)] =
      sendMessages(topic2, (10 to 19).map(_.toString).toArray, None)
    val first = t2mid.head._2
    val last = t2mid.last._2

    // Test explicitly specified offsets
    val startPartitionOffsets = Map(
      topic1 -> MessageId.earliest,
      topic2 -> first // explicit offset happens to = the first
    )
    val startingOffsets = JsonUtils.topicOffsets(startPartitionOffsets)

    val endPartitionOffsets = Map(
      topic1 -> MessageId.latest, // -1 => latest
      topic2 -> last) // explicit offset happens to = the latest

    val endingOffsets = JsonUtils.topicOffsets(endPartitionOffsets)
    val df = createDF(
      s"$topic1,$topic2",
      withOptions = Map("startingOffsets" -> startingOffsets, "endingOffsets" -> endingOffsets))
    checkAnswer(df, (0 to 19).map(_.toString).toDF)

    // static offset partition 2, nothing should change
    sendMessages(topic2, (31 to 39).map(_.toString).toArray, None)
    checkAnswer(df, (0 to 19).map(_.toString).toDF)

    // latest offset partition 1, should change
    sendMessages(topic1, (20 to 29).map(_.toString).toArray, None)
    checkAnswer(df, (0 to 29).map(_.toString).toDF)
  }

  test("reuse same dataframe in query") {
    // This test ensures that we do not cache the Pulsar Consumer in PulsarRelation
    val topic = newTopic()
    sendMessages(topic, (0 to 10).map(_.toString).toArray, None)

    // Specify explicit earliest and latest offset values
    val df = createDF(
      topic,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))
    checkAnswer(df.union(df), ((0 to 10) ++ (0 to 10)).map(_.toString).toDF)
  }

  private def check[T: ClassTag](
                                  schemaType: SchemaType,
                                  datas: Seq[T],
                                  encoder: Encoder[T],
                                  str: T => String): (DataFrame, DataFrame) = {
    val topic = newTopic()
    sendTypedMessages[T](topic, schemaType, datas, None)

    val df = createDF(
      topic,
      withOptions = Map("startingOffsets" -> "earliest", "endingOffsets" -> "latest"))

    val df2 = if (str == null) {
      spark.createDataset(datas)(encoder).toDF().selectExpr("CAST(value as String)")
    } else {
      spark.createDataset(datas.map(str))(Encoders.STRING).toDF()
    }
    checkAnswer(df, df2)
    (df, df2)
  }

  test("test boolean") {
    check[Boolean](SchemaType.BOOLEAN, booleanSeq, Encoders.scalaBoolean, null)
  }

  test("test int") {
    check[Int](SchemaType.INT32, int32Seq, Encoders.scalaInt, null)
  }

  test("test string") {
    check[String](SchemaType.STRING, stringSeq, Encoders.STRING, null)
  }

  test("test byte") {
    check[Byte](SchemaType.INT8, int8Seq, Encoders.scalaByte, null)
  }

  test("test double") {
    check[Double](SchemaType.DOUBLE, doubleSeq, Encoders.scalaDouble, null)
  }

  test("test float") {
    check[Float](SchemaType.FLOAT, floatSeq, Encoders.scalaFloat, null)
  }

  test("test short") {
    check[Short](SchemaType.INT16, int16Seq, Encoders.scalaShort, null)
  }

  test("test long") {
    check[Long](SchemaType.INT64, int64Seq, Encoders.scalaLong, null)
  }

  test("test byte array") {
    // compare string, encoders are just placeholders, not actually used
    check[Array[Byte]](SchemaType.BYTES, bytesSeq, Encoders.BINARY, new String(_))
  }

  test("test date") {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
    check[Date](SchemaType.DATE, dateSeq, Encoders.bean(classOf[Date]), dateFormat.format(_))
  }

  test("test timestamp") {
    val tsFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    check[java.sql.Timestamp](
      SchemaType.TIMESTAMP,
      timestampSeq,
      Encoders.kryo(classOf[java.sql.Timestamp]),
      tsFormat.format(_))
  }

  test("test struct types in avro") {
    import SchemaData._

    val topic = newTopic()

    sendTypedMessages[Foo](topic, SchemaType.AVRO, fooSeq, None)

    val result = spark.read
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_MULTI, topic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(ENDING_OFFSETS_OPTION_KEY, "latest")
      .load()
      .selectExpr("i", "f", "bar")

    checkAnswer(result, fooSeq.toDF())
  }

  test("test struct type with multiple avro version") {
    import SchemaData._

    val topic = newTopic()

    sendTypedMessages[Foo](topic, SchemaType.AVRO, fooSeq, None)
    sendTypedMessages[FooV2](topic, SchemaType.AVRO, fooSeqV2, None)

    val fooSeqV2AsV1 = fooSeqV2.map(fooV2 => Foo(fooV2.i, fooV2.f, null))
    val fooSeqV1All = fooSeq ++ fooSeqV2AsV1

    val fooSeqV1AsV2 = fooSeq.map(foo => FooV2(foo.i, foo.f))
    val fooSeqV2All = fooSeqV1AsV2 ++ fooSeqV2

    val reader = spark.read
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_MULTI, topic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(ENDING_OFFSETS_OPTION_KEY, "latest")

    val resultV1 = reader
      .option(TOPIC_VERSION, "0")
      .load()
      .selectExpr("i", "f", "bar")

    checkAnswer(resultV1, fooSeqV1All.toDF())

    val resultV2 = reader
      .load()
      .selectExpr("i", "f")

    checkAnswer(resultV2, fooSeqV2All.toDF())

  }

  test("test struct types in json") {
    import SchemaData._

    val topic = newTopic()

    sendTypedMessages[F1](topic, SchemaType.JSON, f1Seq, None)

    val result = spark.read
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_MULTI, topic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(ENDING_OFFSETS_OPTION_KEY, "latest")
      .load()
      .selectExpr("baz.f", "baz.d", "baz.mp", "baz.arr")

    checkAnswer(result, f1Results.toDF())
  }

  test("bad batch query options") {
    def testBadOptions(options: (String, String)*)(expectedMsgs: String*): Unit = {
      val ex = intercept[IllegalArgumentException] {
        val reader = spark.read
          .format("pulsar")
          .option(SERVICE_URL_OPTION_KEY, serviceUrl)
          .option(ADMIN_URL_OPTION_KEY, adminUrl)
        options.foreach { case (k, v) => reader.option(k, v) }
        reader.load().collect()
      }
      expectedMsgs.foreach { m =>
        assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(m.toLowerCase(Locale.ROOT)))
      }
    }

    testBadOptions("startingOffsets" -> "earliest")(
      "You should specify topic(s) using one of the topic options:"
    )

    // Specifying an ending offset as the starting point
    testBadOptions(TOPIC_SINGLE -> "t", "startingOffsets" -> "latest")(
      "starting offset can't be latest " +
        "for batch queries on pulsar")

    // Now do it with an explicit json start offset indicating latest
    val startPartitionOffsets = Map("t" -> MessageId.latest)
    val startingOffsets = JsonUtils.topicOffsets(startPartitionOffsets)
    testBadOptions(TOPIC_SINGLE -> "t", "startingOffsets" -> startingOffsets)(
      "starting offset for t can't be latest for batch queries on pulsar")

    // Make sure we catch ending offsets that indicate earliest
    testBadOptions(TOPIC_SINGLE -> "t", "endingOffsets" -> "earliest")(
      "ending offset can't be" +
        " earliest for batch queries on pulsar")

    // Make sure we catch ending offsets that indicating earliest
    val endPartitionOffsets = Map("t" -> MessageId.earliest)
    val endingOffsets = JsonUtils.topicOffsets(endPartitionOffsets)
    testBadOptions(TOPIC_SINGLE -> "t", "endingOffsets" -> endingOffsets)(
      "ending offset for t can't be earliest for batch queries on pulsar")

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
}
