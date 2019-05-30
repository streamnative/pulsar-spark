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
import java.util.{Map => JMap}

import scala.collection.JavaConverters._

import com.google.common.collect.Sets

import io.streamnative.tests.pulsar.service.{PulsarService, PulsarServiceFactory, PulsarServiceSpec}
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{MessageId, PulsarClient}

import org.apache.spark.SparkFunSuite
import org.scalatest.BeforeAndAfterAll

/**
 * A trait to clean cached Pulsar producers in `afterAll`
 */
trait PulsarTest extends BeforeAndAfterAll {
  self: SparkFunSuite =>

  var pulsarService : PulsarService = _
  var serviceUrl: String = _
  var adminUrl: String = _

  override def beforeAll(): Unit = {
    val spec: PulsarServiceSpec = PulsarServiceSpec.builder()
      .clusterName("standalone")
      .enableContainerLogging(false)
      .build()

    pulsarService = PulsarServiceFactory.createPulsarService(spec)
    pulsarService.start()

    val uris = pulsarService
      .getServiceUris.asScala
      .filter(_ != null).partition(_.getScheme == "pulsar")

    serviceUrl = uris._1(0).toString
    adminUrl = uris._2(0).toString

    val admin = PulsarAdmin.builder()
        .serviceHttpUrl(adminUrl)
        .build()
    admin.namespaces().createNamespace("public/default", Sets.newHashSet("standalone"))
    admin.close()

    logInfo(s"Successfully started pulsar service at cluster ${spec.clusterName}")

    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
    CachedPulsarClient.clear()
    CachedPulsarProducer.clear()
    if (pulsarService != null) {
      pulsarService.stop()
    }
  }

  def getAllTopicsSize(): Seq[(String, MessageId)] = {
    val admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()
    val tps = admin.namespaces().getTopics("public/default").asScala
    val lst = tps.map { tp =>
      (tp, PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp)))
    }
    admin.close()
    lst
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

    val client = PulsarClient.builder()
      .serviceUrl(serviceUrl)
      .build()

    val producer = client.newProducer().topic(topic).create()

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

  def getEarliestOffsets(topics: Set[String]): Map[String, MessageId] = {
    val client = PulsarClient.builder()
      .serviceUrl(serviceUrl)
      .build()
    val t2id = topics.map { tp =>
      val consumer = client.newReader().startMessageId(MessageId.earliest).create()
      val mid = consumer.readNext().getMessageId
      consumer.close()
      (tp, mid)
    }.toMap
    client.close()
    t2id
  }

  def getLatestOffsets(topics: Set[String]): Map[String, MessageId] = {
    val admin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()

    val t2id = topics.map { tp =>
      (tp, PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp)))
    }.toMap
    admin.close()
    t2id
  }
}
