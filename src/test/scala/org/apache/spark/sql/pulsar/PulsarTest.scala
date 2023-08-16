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

import java.lang.{Integer => JInt}
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8
import java.time.{Clock, Duration}
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach}

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.testcontainers.containers.PulsarContainer
import org.testcontainers.utility.DockerImageName.parse
import org.testcontainers.containers.output.Slf4jLogConsumer

import org.apache.pulsar.client.admin.{PulsarAdmin, PulsarAdminException}
import org.apache.pulsar.client.api.{MessageId, Producer, PulsarClient, Schema}
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.protocol.schema.PostSchemaPayload
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}
import org.apache.spark.SparkFunSuite
import org.apache.spark.util.Utils

import java.util.concurrent.atomic.AtomicInteger

/**
 * A trait to clean cached Pulsar producers in `afterAll`
 */
trait PulsarTest extends BeforeAndAfterAll with BeforeAndAfterEach {
  self: SparkFunSuite =>
  import PulsarOptions._

  val CURRENT_VERSION = "2.10.2"

  var pulsarContainer: PulsarContainer = null
  var serviceUrl: String = null
  var adminUrl: String = null

  private val logger: Logger = LoggerFactory.getLogger("pulsar-spark-test-logger")

  override def beforeAll(): Unit = {
    pulsarContainer = new PulsarContainer(parse("apachepulsar/pulsar:" + CURRENT_VERSION))
    pulsarContainer.withStartupTimeout(Duration.ofMinutes(5))
    pulsarContainer.start()


    serviceUrl = pulsarContainer.getPulsarBrokerUrl()
    adminUrl = pulsarContainer.getHttpServiceUrl()
    pulsarContainer.followOutput(new Slf4jLogConsumer(logger, true))

    super.beforeAll()
  }

  private val subscriptionId = new AtomicInteger(0)

  protected def newSubscription(): String = TopicName.get(
    s"subscription-${subscriptionId.getAndIncrement()}").toString

  override def afterAll(): Unit = {
    super.afterAll()
    CachedPulsarClient.clear()
    if (pulsarContainer != null) {
      pulsarContainer.stop()
      pulsarContainer.close()
    }
  }

  protected override def afterEach(): Unit = {
    super.afterEach()

    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      admin.topics().getPartitionedTopicList("public/default").asScala.foreach { tp =>
        admin.topics().deletePartitionedTopic(tp, true)
      }

      admin.topics().getList("public/default").asScala.foreach { tp =>
        admin.topics().delete(tp, true)
      }
    }
  }

  def getAllTopicsSize(): Seq[(String, MessageId)] = {
    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      val tps = admin.namespaces().getTopics("public/default").asScala
      tps.map { tp =>
        (tp, PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp)))
      }
    }
  }

  /** Java-friendly function for sending messages to the Pulsar */
  def sendMessages(topic: String, messageToFreq: JMap[String, JInt]): Unit = {
    sendMessages(topic, Map(messageToFreq.asScala.mapValues(_.intValue()).toSeq: _*))
  }

  /** Send the messages to the Pulsar */
  def sendMessages(topic: String, messageToFreq: Map[String, Int]): Unit = {
    val messages = messageToFreq.flatMap { case (s, freq) => Seq.fill(freq)(s) }.toArray
    sendMessages(topic, messages)
  }

  /** Send the array of messages to the Pulsar */
  def sendMessages(topic: String, messages: Array[String]): Seq[(String, MessageId)] = {
    sendMessages(topic, messages, None)
  }

  /** Send the array of messages to the Pulsar using specified partition */
  def sendMessages(
      topic: String,
      messages: Array[String],
      partition: Option[Int]): Seq[(String, MessageId)] = {

    val topicName = if (partition.isEmpty) topic else s"$topic$PartitionSuffix${partition.get}"

    val client = PulsarClient
      .builder()
      .serviceUrl(serviceUrl)
      .build()

    val producer = client.newProducer().topic(topicName).create()

    val offsets = try {
      messages.map { m =>
        val mid = producer.send(m.getBytes(StandardCharsets.UTF_8))
        logInfo(s"\t Sent $m of mid: $mid")
        (m, mid)
      }
    } finally {
      producer.flush()
      producer.close()
      client.close()
    }
    offsets
  }

  /** Send the array of messages to the Pulsar using specified partition */
  def sendMessagesWithClock(
      topic: String,
      messages: Array[String],
      partition: Option[Int],
      clock: Clock): Seq[(String, MessageId)] = {

    val topicName = if (partition.isEmpty) topic else s"$topic$PartitionSuffix${partition.get}"

    val client = PulsarClient
      .builder()
      .clock(clock)
      .serviceUrl(serviceUrl)
      .build()

    val producer = client.newProducer().topic(topicName).create()

    val offsets = try {
      messages.map { m =>
        val mid = producer.send(m.getBytes(StandardCharsets.UTF_8))
        logInfo(s"\t Sent $m of mid: $mid")
        (m, mid)
      }
    } finally {
      producer.flush()
      producer.close()
      client.close()
    }
    offsets
  }

  def sendTypedMessages[T: ClassTag](
      topic: String,
      tpe: SchemaType,
      messages: Seq[T],
      partition: Option[Int]): Seq[MessageId] = {

    val topicName = if (partition.isEmpty) topic else s"$topic$PartitionSuffix${partition.get}"

    val client = PulsarClient
      .builder()
      .serviceUrl(serviceUrl)
      .build()

    val producer: Producer[T] = tpe match {
      case SchemaType.BOOLEAN =>
        client.newProducer(Schema.BOOL).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.BYTES =>
        client.newProducer(Schema.BYTES).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.DATE =>
        client.newProducer(Schema.DATE).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.STRING =>
        client.newProducer(Schema.STRING).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.TIMESTAMP =>
        client.newProducer(Schema.TIMESTAMP).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.INT8 =>
        client.newProducer(Schema.INT8).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.DOUBLE =>
        client.newProducer(Schema.DOUBLE).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.FLOAT =>
        client.newProducer(Schema.FLOAT).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.INT32 =>
        client.newProducer(Schema.INT32).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.INT64 =>
        client.newProducer(Schema.INT64).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.INT16 =>
        client.newProducer(Schema.INT16).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.AVRO =>
        val cls = implicitly[ClassTag[T]].runtimeClass
        client.newProducer(Schema.AVRO(cls)).topic(topicName).create().asInstanceOf[Producer[T]]
      case SchemaType.JSON =>
        val cls = implicitly[ClassTag[T]].runtimeClass
        client.newProducer(Schema.JSON(cls)).topic(topicName).create().asInstanceOf[Producer[T]]
      case _ => throw new NotImplementedError(s"not supported type $tpe")
    }

    val offsets = try {
      messages.map { m =>
        val mid = producer.send(m)
        logInfo(s"\t Sent $m of mid: $mid")
        mid
      }
    } finally {
      producer.flush()
      producer.close()
      client.close()
    }
    offsets
  }

  def getEarliestOffsets(topics: Set[String]): Map[String, MessageId] = {
    val client = PulsarClient
      .builder()
      .serviceUrl(serviceUrl)
      .build()
    val t2id = topics.map { tp =>
      val reader = client.newReader().startMessageId(MessageId.earliest).create()
      val mid = reader.readNext().getMessageId
      reader.close()
      (tp, mid)
    }.toMap
    client.close()
    t2id
  }

  def getLatestOffsets(topics: Set[String]): Map[String, MessageId] = {
    val client = PulsarClient
      .builder()
      .serviceUrl(serviceUrl)
      .build()

    val topicPartitions = topics.flatMap { tp =>
      client.getPartitionsForTopic(tp).get().asScala
    }
    val subscription = newSubscription()
    val offsets = topicPartitions.map { tp =>
      val mid = CachedConsumer.getOrCreate(tp, subscription, client).getLastMessageId
      tp -> mid
    }.toMap
    client.close()
    topicPartitions.foreach(CachedConsumer.close(_, subscription))
    offsets
  }

  def addPartitions(topic: String, partitions: Int): Unit = {
    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      admin.topics().updatePartitionedTopic(topic, partitions)
    }
  }

  def createNonPartitionedTopic(topic: String): Unit = {
    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      admin.topics().createNonPartitionedTopic(topic)
    }
  }

  def createTopic(topic: String, partitions: Int): Unit = {
    assert(partitions > 1)
    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      admin.topics().createPartitionedTopic(topic, partitions)
    }
  }

  def deleteTopic(topic: String): Unit = {
    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      val partitions = admin.topics().getPartitionedTopicMetadata(topic).partitions
      if (partitions > 0) {
        admin.topics().deletePartitionedTopic(topic, true)
      } else {
        admin.topics().delete(topic, true)
      }
    }
  }

  /**
   * Wait until the latest offset of the given `topic` is not less than `offset`.
   */
  def waitUntilOffsetAppears(topic: String, offset: MessageId): Unit = {
    import org.scalatest.time.SpanSugar._

    eventually(timeout(60.seconds)) {
      val currentOffset = getLatestOffsets(Set(topic)).get(topic)
      assert(currentOffset.nonEmpty && currentOffset.get.compareTo(offset) >= 0)
    }
  }

  def createPulsarSchema(topic: String, schemaInfo: SchemaInfo): Unit = {
    assert(schemaInfo != null, "schemaInfo shouldn't be null")
    val pl = new PostSchemaPayload()
    pl.setType(schemaInfo.getType.name())
    pl.setSchema(new String(schemaInfo.getSchema, UTF_8))
    pl.setProperties(schemaInfo.getProperties)
    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      try {
        admin.schemas().createSchema(TopicName.get(topic).toString, pl)
      } catch {
        case e: PulsarAdminException if e.getStatusCode == 404 =>
          logError(s"Create schema for ${TopicName.get(topic).toString} got 404")
        case e: Throwable =>
          throw new RuntimeException(
            s"Failed to create schema for ${TopicName.get(topic).toString}",
            e)
      }
    }
  }
}
