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

import java.util.UUID
import java.util.regex.Pattern
import java.{util => ju}

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{MessageId, PulsarClient, SubscriptionType}
import org.apache.pulsar.common.naming.TopicName

import org.apache.spark.internal.Logging
import org.apache.spark.sql.pulsar.PulsarOptions.TOPIC_OPTION_KEYS

/**
  * A Helper class that responsible for:
  * - getEarliest / Latest / Specific MessageIds
  * - guarantee message existence using subscription by setup, move and remove
  */
private[pulsar] case class PulsarMetadataReader(
    serviceUrl: String,
    adminUrl: String,
    clientConf: ju.Map[String, Object],
    driverGroupIdPrefix: String,
    caseInsensitiveParameters: Map[String, String]) extends Logging {

  import scala.collection.JavaConverters._

  protected val admin: PulsarAdmin =
    PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()
  protected lazy val client: PulsarClient =
    PulsarClient.builder().serviceUrl(serviceUrl).build()

  private var topics: Seq[String] = _

  def stop(): Unit = {
    admin.close()
    if (client != null) {
      client.close()
    }
  }

  def setupCursor(offset: SpecificPulsarOffset): Unit = {
    offset.topicOffsets.foreach { case (tp, mid) =>
      admin.topics().createSubscription(tp, s"$driverGroupIdPrefix-$tp", mid)
    }
  }

  def commitCursorToOffset(offset: Map[String, MessageId]): Unit = {
    offset.foreach { case (tp, mid) =>
      admin.topics().resetCursor(tp, s"$driverGroupIdPrefix-$tp", mid)
    }
  }

  def removeCursor(): Unit = {
    getTopics()
    topics.foreach { tp =>
      admin.topics().deleteSubscription(tp, s"$driverGroupIdPrefix-$tp")
    }
  }

  def fetchLatestOffsets(): SpecificPulsarOffset = {
    getTopics()
    SpecificPulsarOffset(
      topics.map { tp =>
        (tp -> PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp)))
      }.toMap)
  }

  def fetchLatestOffsetForTopic(topic: String): MessageId = {
    PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(topic))
  }

  private def getTopics(): Seq[String] = {
    topics = caseInsensitiveParameters.find(x => TOPIC_OPTION_KEYS.contains(x._1)).get match {
      case ("topic", value) =>
        TopicName.get(value).toString :: Nil
      case ("topics", value) =>
        value.split(",").map(_.trim).filter(_.nonEmpty).map(TopicName.get(_).toString)
      case ("topicspattern", value) =>
        getTopics(value)
    }
    topics
  }

  private def getTopics(topicsPattern: String): Seq[String] = {
    val dest = TopicName.get(topicsPattern)
    val allTopics: ju.List[String] = admin.topics().getList(dest.getNamespace)
    topicsPatternFilter(allTopics, dest.toString)
  }

  private def topicsPatternFilter(allTopics: ju.List[String], topicsPattern: String): Seq[String] = {
    val shortenedTopicsPattern = Pattern.compile(topicsPattern.split("\\:\\/\\/")(1))
    allTopics.asScala
      .map(TopicName.get(_).toString)
      .filter(tp => shortenedTopicsPattern.matcher(tp.split("\\:\\/\\/")(1)).matches())
  }

  def offsetForEachTopic(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: PulsarOffset): SpecificPulsarOffset = {

    getTopics()
    val offset = PulsarProvider.getPulsarOffset(params, offsetOptionKey, defaultOffsets)
    offset match {
      case LatestOffset =>
        SpecificPulsarOffset(topics.map(tp => (tp, MessageId.latest)).toMap)
      case EarliestOffset =>
        SpecificPulsarOffset(topics.map(tp => (tp, MessageId.earliest)).toMap)
      case so: SpecificPulsarOffset =>
        val specified: Map[String, MessageId] = so.topicOffsets
        assert(specified.keySet.subsetOf(topics.toSet),
          s"topics designated in startingOffsets/endingOffsets should all appear in $TOPIC_OPTION_KEYS .\n" +
            s"topics: $topics, topics in offsets: ${specified.keySet}")
        val nonSpecifiedTopics = topics.toSet -- specified.keySet
        val nonSpecified = nonSpecifiedTopics.map { tp =>
          defaultOffsets match {
            case LatestOffset => (tp, MessageId.latest)
            case EarliestOffset => (tp, MessageId.earliest)
            case _ => throw new IllegalArgumentException("Defaults should be latest or earliest")
          }
        }.toMap
        SpecificPulsarOffset(specified ++ nonSpecified)
    }
  }

  def fetchCurrentOffsets(
      offset: SpecificPulsarOffset, reportDataLoss: String => Unit): Map[String, MessageId] = {

    offset.topicOffsets.map { case (tp, off) =>
      val actualOffset = off match {
        case MessageId.earliest =>
          off
        case MessageId.latest =>
          PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp))
        case _ =>
          val consumer = client.newConsumer()
            .topic(tp)
            .subscriptionName(s"spark-pulsar-${UUID.randomUUID()}")
            .subscriptionType(SubscriptionType.Exclusive)
            .subscribe()
          consumer.seek(off)
          val msg = consumer.receive()
          consumer.close()
          PulsarSourceUtils.mid2Impl(msg.getMessageId)
      }
      (tp, actualOffset)
    }
  }
}
