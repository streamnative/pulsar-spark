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

import java.{util => ju}
import java.io.Closeable
import java.util.{Optional, UUID}
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.apache.pulsar.client.admin.{PulsarAdmin, PulsarAdminException}
import org.apache.pulsar.client.api.{Message, MessageId, PulsarClient, SubscriptionInitialPosition, SubscriptionType}
import org.apache.pulsar.client.impl.schema.BytesSchema
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.pulsar.PulsarOptions.{AUTH_PARAMS, AUTH_PLUGIN_CLASS_NAME, TLS_ALLOW_INSECURE_CONNECTION, TLS_HOSTNAME_VERIFICATION_ENABLE, TLS_TRUST_CERTS_FILE_PATH, TOPIC_OPTION_KEYS}
import org.apache.spark.sql.types.StructType

/**
 * A Helper class that responsible for:
 * - getEarliest / Latest / Specific MessageIds
 * - guarantee message existence using subscription by setup, move and remove
 */
private[pulsar] case class PulsarMetadataReader(
    serviceUrl: String,
    adminUrl: String,
    adminApiRetrier: Retrier,
    clientConf: ju.Map[String, Object],
    adminClientConf: ju.Map[String, Object],
    driverGroupIdPrefix: String,
    caseInsensitiveParameters: Map[String, String])
    extends Closeable
    with Logging {

  import scala.collection.JavaConverters._

  protected val admin: PulsarAdmin = AdminUtils.buildAdmin(adminUrl, adminClientConf)
  protected var client: PulsarClient = CachedPulsarClient.getOrCreate(clientConf)

  private var topics: Seq[String] = _
  private var topicPartitions: Seq[String] = _

  override def close(): Unit = {
    admin.close()
  }

  def setupCursor(startingPos: PerTopicOffset): Unit = {
    startingPos match {
      case off: SpecificPulsarOffset => setupCursorByMid(off)
      case time: SpecificPulsarStartingTime => setupCursorByTime(time)
      case s => throw new UnsupportedOperationException(s"$s shouldn't appear here, a bug occurs.")
    }
  }

  def setupCursorByMid(offset: SpecificPulsarOffset): Unit = {
    offset.topicOffsets.foreach {
      case (tp, mid) =>
        val umid = mid.asInstanceOf[UserProvidedMessageId]
        try {
          adminApiRetrier.retry {
            admin.topics().createSubscription(tp, s"$driverGroupIdPrefix-$tp", umid.mid)
          }
        } catch {
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to setup cursor for ${TopicName.get(tp).toString}",
              e)
        }
    }
  }

  def setupCursorByTime(time: SpecificPulsarStartingTime): Unit = {
    time.topicTimes.foreach {
      case (tp, time) =>
        try {
          if (time == PulsarProvider.EARLIEST_TIME) {
            adminApiRetrier.retry {
              admin.topics().createSubscription(tp, s"$driverGroupIdPrefix-$tp", MessageId.earliest)
            }
          } else if (time == PulsarProvider.LATEST_TIME) {
            adminApiRetrier.retry {
              admin.topics().createSubscription(tp, s"$driverGroupIdPrefix-$tp", MessageId.latest)
            }
          } else if (time < 0) {
            throw new RuntimeException(s"Invalid starting time for $tp: $time")
          } else {
            adminApiRetrier.retry {
              admin.topics().createSubscription(tp, s"$driverGroupIdPrefix-$tp", MessageId.latest)
            }
            adminApiRetrier.retry {
              admin.topics().resetCursor(tp, s"$driverGroupIdPrefix-$tp", time)
            }
          }
        } catch {
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to setup cursor for ${TopicName.get(tp).toString}", e)
        }
    }
  }

  def commitCursorToOffset(offset: Map[String, MessageId]): Unit = {
    offset.foreach {
      case (tp, mid) =>
        try {
          adminApiRetrier.retry {
            admin.topics().resetCursor(tp, s"$driverGroupIdPrefix-$tp", mid)
          }
        } catch {
          case e: PulsarAdminException if e.getStatusCode == 404 || e.getStatusCode == 412 =>
            logInfo(
              s"Cannot commit cursor since the topic $tp has been deleted during execution.")
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to commit cursor for ${TopicName.get(tp).toString}",
              e)
        }
    }
  }

  def removeCursor(): Unit = {
    getTopics()
    topics.foreach { tp =>
      try {
        adminApiRetrier.retry {
          admin.topics().deleteSubscription(tp, s"$driverGroupIdPrefix-$tp")
        }
      } catch {
        case e: PulsarAdminException if e.getStatusCode == 404 =>
          logInfo(s"Cannot remove cursor since the topic $tp has been deleted during execution.")
        case e: Throwable =>
          throw new RuntimeException(
            s"Failed to remove cursor for ${TopicName.get(tp).toString}",
            e)
      }
    }
  }

  def getAndCheckCompatible(schema: Option[StructType]): StructType = {
    val inferredSchema = getSchema()
    require(
      schema.isEmpty || inferredSchema == schema.get,
      "The Schema of Pulsar source and provided doesn't match")
    inferredSchema
  }

  def getAndCheckCompatible(schema: Optional[StructType]): StructType = {
    val inferredSchema = getSchema()
    require(
      !schema.isPresent || inferredSchema == schema.get,
      "The Schema of Pulsar source and provided doesn't match")
    inferredSchema
  }

  def getSchema(): StructType = {
    val si = getPulsarSchema()
    SchemaUtils.pulsarSourceSchema(si)
  }

  def getPulsarSchema(): SchemaInfo = {
    getTopics()
    if (topics.size > 0) {
      val schemas = topics.map { tp =>
        getPulsarSchema(tp)
      }
      val sset = schemas.toSet
      if (sset.size != 1) {
        throw new IllegalArgumentException(
          s"Topics to read must share identical schema, " +
            s"however we got ${sset.size} distinct schemas:[${sset.mkString(", ")}]")
      }
      sset.head
    } else {
      // if no topic exists, and we are getting schema, then auto created topic has schema of None
      SchemaUtils.emptySchemaInfo()
    }
  }

  def getPulsarSchema(topic: String): SchemaInfo = {
    try {
      adminApiRetrier.retry {
        admin.schemas().getSchemaInfo(TopicName.get(topic).toString)
      }
    } catch {
      case e: PulsarAdminException if e.getStatusCode == 404 =>
        return BytesSchema.of().getSchemaInfo
      case e: Throwable =>
        throw new RuntimeException(
          s"Failed to get schema information for ${TopicName.get(topic).toString}",
          e)
    }
  }

  def fetchLatestOffsets(): SpecificPulsarOffset = {
    getTopicPartitions()
    SpecificPulsarOffset(topicPartitions.map { tp =>
      (tp -> PulsarSourceUtils.seekableLatestMid(
        try {
          adminApiRetrier.retry {
            admin.topics().getLastMessageId(tp)
          }
        } catch {
          case e: PulsarAdminException if e.getStatusCode == 404 =>
            MessageId.earliest
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to get last messageId for ${TopicName.get(tp).toString}",
              e)
        }
      ))
    }.toMap)
  }

  def fetchLatestOffsetForTopic(topic: String): MessageId = {
    PulsarSourceUtils.seekableLatestMid( try {
      adminApiRetrier.retry {
        admin.topics().getLastMessageId(topic)
      }
    } catch {
      case e: PulsarAdminException if e.getStatusCode == 404 =>
        MessageId.earliest
      case e: Throwable =>
        throw new RuntimeException(
          s"Failed to get last messageId for ${TopicName.get(topic).toString}",
          e)
    })
  }

  def fetchEarliestOffsets(topics: Seq[String]): Map[String, MessageId] = {
    if (topics.isEmpty) {
      Map.empty[String, MessageId]
    } else {
      topics.map(p => p -> MessageId.earliest).toMap
    }
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

  private def getTopicPartitions(): Seq[String] = {
    getTopics()
    topicPartitions = topics.flatMap { tp =>
      val partNum = adminApiRetrier.retry{
        admin.topics().getPartitionedTopicMetadata(tp).partitions
      }
      if (partNum == 0) {
        tp :: Nil
      } else {
        (0 until partNum).map(tp + PulsarOptions.PARTITION_SUFFIX + _)
      }
    }
    topicPartitions
  }

  private def getTopics(topicsPattern: String): Seq[String] = {
    val dest = TopicName.get(topicsPattern)
    val allNonPartitionedTopics: ju.List[String] =
      admin
        .topics()
        .getList(dest.getNamespace)
        .asScala
        .filter(t => !TopicName.get(t).isPartitioned)
        .asJava
    val nonPartitionedMatch = topicsPatternFilter(allNonPartitionedTopics, dest.toString)

    val allPartitionedTopics: ju.List[String] =
      adminApiRetrier.retry{
        admin.topics().getPartitionedTopicList(dest.getNamespace)
      }
    val partitionedMatch = topicsPatternFilter(allPartitionedTopics, dest.toString)
    nonPartitionedMatch ++ partitionedMatch
  }

  private def topicsPatternFilter(
      allTopics: ju.List[String],
      topicsPattern: String): Seq[String] = {
    val shortenedTopicsPattern = Pattern.compile(topicsPattern.split("\\:\\/\\/")(1))
    allTopics.asScala
      .map(TopicName.get(_).toString)
      .filter(tp => shortenedTopicsPattern.matcher(tp.split("\\:\\/\\/")(1)).matches())
  }

  def startingOffsetForEachTopic(
      params: Map[String, String],
      defaultOffsets: PulsarOffset): PerTopicOffset = {
    getTopicPartitions()

    val startingOffset = PulsarProvider.getPulsarStartingOffset(params, defaultOffsets)
    startingOffset match {
      case LatestOffset =>
        SpecificPulsarOffset(
          topicPartitions.map(tp => (tp, UserProvidedMessageId(MessageId.latest))).toMap)
      case EarliestOffset =>
        SpecificPulsarOffset(
          topicPartitions.map(tp => (tp, UserProvidedMessageId(MessageId.earliest))).toMap)
      case so: SpecificPulsarOffset =>
        val specified: Map[String, MessageId] = so.topicOffsets.map {
          case (tp, mid) => (tp, UserProvidedMessageId(mid)) }
        assert(
          specified.keySet.subsetOf(topicPartitions.toSet),
          s"topics designated in startingOffsets/endingOffsets" +
            s" should all appear in $TOPIC_OPTION_KEYS .\n" +
            s"topics: $topicPartitions, topics in offsets: ${specified.keySet}"
        )
        val nonSpecifiedTopics = topicPartitions.toSet -- specified.keySet
        val nonSpecified = nonSpecifiedTopics.map { tp =>
          defaultOffsets match {
            case LatestOffset => (tp, UserProvidedMessageId(MessageId.latest))
            case EarliestOffset => (tp, UserProvidedMessageId(MessageId.earliest))
            case _ => throw new IllegalArgumentException("Defaults should be latest or earliest")
          }
        }.toMap
        SpecificPulsarOffset(specified ++ nonSpecified)

      case TimeOffset(ts) =>
        SpecificPulsarStartingTime(topicPartitions.map(tp => (tp, ts)).toMap)
      case st: SpecificPulsarStartingTime =>
        val specified: Map[String, Long] = st.topicTimes
        assert(
          specified.keySet.subsetOf(topicPartitions.toSet),
          s"topics designated in startingTime" +
            s" should all appear in $TOPIC_OPTION_KEYS .\n" +
            s"topics: $topicPartitions, topics in startingTime: ${specified.keySet}"
        )
        val nonSpecifiedTopics = topicPartitions.toSet -- specified.keySet
        val nonSpecified: Map[String, Long] = nonSpecifiedTopics.map { tp =>
          defaultOffsets match {
            case LatestOffset => (tp, PulsarProvider.LATEST_TIME)
            case EarliestOffset => (tp, PulsarProvider.EARLIEST_TIME)
            case _ => throw new IllegalArgumentException("Defaults should be latest or earliest")
          }
        }.toMap
        SpecificPulsarStartingTime(specified ++ nonSpecified)
    }
  }

  def offsetForEachTopic(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: PulsarOffset): SpecificPulsarOffset = {

    getTopicPartitions()
    val offset = PulsarProvider.getPulsarOffset(params, offsetOptionKey, defaultOffsets)
    offset match {
      case LatestOffset =>
        SpecificPulsarOffset(topicPartitions.map(tp => (tp, MessageId.latest)).toMap)
      case EarliestOffset =>
        SpecificPulsarOffset(topicPartitions.map(tp => (tp, MessageId.earliest)).toMap)
      case so: SpecificPulsarOffset =>
        val specified: Map[String, MessageId] = so.topicOffsets
        assert(
          specified.keySet.subsetOf(topicPartitions.toSet),
          s"topics designated in startingOffsets/endingOffsets" +
            s" should all appear in $TOPIC_OPTION_KEYS .\n" +
            s"topics: $topicPartitions, topics in offsets: ${specified.keySet}"
        )
        val nonSpecifiedTopics = topicPartitions.toSet -- specified.keySet
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

  def actualOffsets(
      offset: PerTopicOffset,
      pollTimeoutMs: Int,
      reportDataLoss: String => Unit): Map[String, MessageId] = {

    offset match {
      case so: SpecificPulsarOffset => fetchCurrentOffsets(so, pollTimeoutMs, reportDataLoss)
      case st: SpecificPulsarStartingTime => fetchCurrentOffsets(st, pollTimeoutMs, reportDataLoss)
      case t => throw new IllegalArgumentException(s"not supported offset type: $t")
    }
  }

  def fetchCurrentOffsets(
      time: SpecificPulsarStartingTime,
      pollTimeoutMs: Int,
      reportDataLoss: String => Unit): Map[String, MessageId] = {

    time.topicTimes.map { case (tp, time) =>
      val actualOffset =
        if (time == PulsarProvider.EARLIEST_TIME) {
          UserProvidedMessageId(MessageId.earliest)
        } else if (time == PulsarProvider.LATEST_TIME) {
          UserProvidedMessageId(
            PulsarSourceUtils.seekableLatestMid(
              adminApiRetrier.retry{
                admin.topics().getLastMessageId(tp)
              }
            )
          )
        } else {
          assert (time > 0, s"time less than 0: $time")
          val reader = client
            .newReader()
            .topic(tp)
            .startMessageId(MessageId.earliest)
            .startMessageIdInclusive()
            .create()

          var earliestMessage: Message[Array[Byte]] = null
          earliestMessage = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
          if (earliestMessage == null) {
            UserProvidedMessageId(MessageId.earliest)
          } else {
            val earliestId = earliestMessage.getMessageId

            reader.seek(time)
            var msg: Message[Array[Byte]] = null
            msg = reader.readNext(pollTimeoutMs, TimeUnit.MILLISECONDS)
            if (msg == null) {
              UserProvidedMessageId(MessageId.earliest)
            } else {
              if (msg.getMessageId == earliestId)  {
                UserProvidedMessageId(MessageId.earliest)
              } else {
                // intentionally leave this id from UserProvided since it's the last id
                // less than time, need to skip this one at the beginning
                PulsarSourceUtils.mid2Impl(msg.getMessageId)
              }
            }
          }
        }
      (tp, actualOffset)
    }
  }

  def fetchCurrentOffsets(
      offset: SpecificPulsarOffset,
      poolTimeoutMs: Int,
      reportDataLoss: String => Unit): Map[String, MessageId] = {

    offset.topicOffsets.map {
      case (tp, off) =>
        val actualOffset = fetchOffsetForTopic(poolTimeoutMs, reportDataLoss, tp, off)
        (tp, actualOffset)
    }
  }

  private def fetchOffsetForTopic(
      poolTimeoutMs: Int,
      reportDataLoss: String => Unit,
      tp: String,
      off: MessageId): MessageId = {
    off match {
      case UserProvidedMessageId(mid) if mid == MessageId.earliest =>
        UserProvidedMessageId(mid)
      case UserProvidedMessageId(mid) if mid == MessageId.latest =>
        UserProvidedMessageId(mid)
      case UserProvidedMessageId(mid) =>
        fetchOffsetForTopic(poolTimeoutMs, reportDataLoss, tp, mid)
      case MessageId.earliest =>
        UserProvidedMessageId(off)
      case MessageId.latest =>
        UserProvidedMessageId(
          PulsarSourceUtils.seekableLatestMid(
            adminApiRetrier.retry{
              admin.topics().getLastMessageId(tp)
            }
          )
        )
      case _ =>
        val reader = client
          .newReader()
          .startMessageId(off)
          .startMessageIdInclusive()
          .topic(tp)
          .create()
        var msg: Message[Array[Byte]] = null
        msg = reader.readNext(poolTimeoutMs, TimeUnit.MILLISECONDS)
        reader.close()
        if (msg == null) {
          reportDataLoss(s"The starting offset provided is not available: $tp, $off")
          UserProvidedMessageId(MessageId.earliest)
        } else {
          UserProvidedMessageId(PulsarSourceUtils.mid2Impl(msg.getMessageId))
        }
    }
  }
}
