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
import java.util.UUID
import java.util.concurrent.atomic.AtomicInteger

import org.apache.pulsar.client.api.{PulsarClient, Schema, SubscriptionInitialPosition}
import org.apache.pulsar.segment.test.common.PulsarServiceResource
import org.apache.spark.sql.execution.streaming.MemoryStream
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamTest, StreamingQuery}
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.time.SpanSugar._

import scala.collection.JavaConversions._
import scala.collection.mutable

class PulsarSinkSuite extends StreamTest with SharedSQLContext with PulsarTest {
  import testImplicits._

  protected var pulsarResource: PulsarServiceResource = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    pulsarResource = new PulsarServiceResource()
    pulsarResource.setup()
  }

  override def afterAll(): Unit = {
    try {
      if (pulsarResource != null) {
        pulsarResource.teardown()
        pulsarResource = null
      }
    } finally {
      super.afterAll()
    }
  }

  /**
  test("batch - write to pulsar") {
    val topic = newTopic()
    val df = Seq("1", "2", "3", "4", "5") map { v =>
      (v, v)
    } toDF("key", "value")
    df.write
      .format("pulsar")
      .option(
        s"${PulsarOptions.SPARK_PULSAR_COMMON_OPTION_KEY_PREFIX}${PulsarOptions.SERVICE_URL_OPTION_KEY}",
        pulsarResource.getBrokerServiceUrl)
      .option(
        s"${PulsarOptions.SPARK_PULSAR_SINK_OPTION_KEY_PREFIX}${PulsarOptions.TOPIC_OPTION_KEY}",
        topic)
      .save()
    val receivedKVs = verifyReceivedMessages(topic, 5)
    assert(5 == receivedKVs._1.size)
    assert(5 == receivedKVs._2.size)
    1.to(5) foreach { i =>
      assert(receivedKVs._1.contains(s"${i}"))
      assert(receivedKVs._2.contains(s"${i}"))
    }
  }

  test("streaming - write aggregation") {
    val input = MemoryStream[String]
    val topic = newTopic()

    createPulsarSubscription(topic, "keepalive")

    val writer = createPulsarWriter(
      input.toDF().groupBy("value").count(),
      withTopic = Some(topic),
      withOutputMode = Some(OutputMode.Update()))(
      withSelectExpr = "CAST(value as STRING) key", "CAST(count as STRING) value"
    )

    input.addData("1", "2", "2", "3", "3", "3")
    failAfter(streamingTimeout) {
      writer.processAllAvailable()
    }
    checkTopicUnorderly(
      topic, 3, ("1", "1"), ("2", "2"), ("3", "3"))

    input.addData("1", "2", "3")
    failAfter(streamingTimeout) {
      writer.processAllAvailable()
    }
    checkTopicUnorderly(
      topic, 6,
      ("1", "1"), ("2", "2"), ("3", "3"),
      ("1", "2"), ("2", "3"), ("3", "4")
    )
  }
    **/

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def createPulsarSubscription(
      topic: String,
      subscription: String): Unit = {
    val client = PulsarClient.builder()
      .serviceUrl(pulsarResource.getBrokerServiceUrl)
      .build()

    val consumer = client.newConsumer(Schema.BYTES)
      .topic(topic)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscriptionName(subscription)
      .subscribe()

    consumer.close()
    client.close()
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
        .option(
          s"${PulsarOptions.SPARK_PULSAR_COMMON_OPTION_KEY_PREFIX}${PulsarOptions.SERVICE_URL_OPTION_KEY}",
          pulsarResource.getBrokerServiceUrl)
        .queryName("pulsarStream")
      withTopic.foreach(stream.option(
        s"${PulsarOptions.SPARK_PULSAR_SINK_OPTION_KEY_PREFIX}${PulsarOptions.TOPIC_OPTION_KEY}",
        _))
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }

  private def verifyReceivedMessages(topic: String, numMessages: Int): (Set[String], Set[String]) = {
    val client = PulsarClient.builder()
      .serviceUrl(pulsarResource.getBrokerServiceUrl)
      .build()

    val consumer = client.newConsumer(Schema.BYTES)
      .topic(topic)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscriptionName("verifier")
      .subscribe()

    var receivedKeys: mutable.Set[String] = mutable.Set()
    var receivedVals: mutable.Set[String] = mutable.Set()

    1.to(numMessages) map { _ =>
      val msg = consumer.receive()
      logInfo(s"Received : key = ${msg.getKey}, value = ${new String(msg.getValue)}")
      receivedKeys = receivedKeys + new String(msg.getKeyBytes, UTF_8)
      receivedVals = receivedVals + new String(msg.getValue, UTF_8)
    }

    consumer.close()
    client.close()

    (receivedKeys.toSet, receivedVals.toSet)
  }

  private def checkTopicUnorderly(topic: String,
                                  numMessages: Int,
                                  expectedAnswer: (String, String)*): Unit = {
    val client = PulsarClient.builder()
      .serviceUrl(pulsarResource.getBrokerServiceUrl)
      .build()

    val consumer = client.newConsumer(Schema.BYTES)
      .topic(topic)
      .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      .subscriptionName("verifier-" + UUID.randomUUID())
      .subscribe()

    var receivedResults: List[(String, String)] = List()

    1.to(numMessages) map { _ =>
      val msg = consumer.receive()
      logInfo(s"Received : key = ${msg.getKey}, value = ${new String(msg.getValue, UTF_8)}")
      val key = new String(msg.getKeyBytes, UTF_8)
      val value = new String(msg.getValue, UTF_8)
      receivedResults = receivedResults :+ ((key, value))
    }

    consumer.close()
    client.close()

    val resultSeq = receivedResults.sorted
    val expectedSeq = expectedAnswer.toSeq.sorted

    if (!compare(resultSeq, expectedSeq)) {
      fail(
        s"""
           |Decoded objects do not match expected objects:
           |expected: $expectedAnswer
           |actual:   ${resultSeq}
         """.stripMargin)
    }

  }

  private def compare(obj1: Any, obj2: Any): Boolean = (obj1, obj2) match {
    case (null, null) => true
    case (null, _) => false
    case (_, null) => false
    case (a: Array[_], b: Array[_]) =>
      a.length == b.length && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a: Iterable[_], b: Iterable[_]) =>
      a.size == b.size && a.zip(b).forall { case (l, r) => compare(l, r)}
    case (a, b) => a == b
  }

}
