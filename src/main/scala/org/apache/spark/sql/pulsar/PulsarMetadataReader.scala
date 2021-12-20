/*
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
import java.util.Optional
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import org.apache.pulsar.client.admin.{PulsarAdmin, PulsarAdminException}
import org.apache.pulsar.client.api.{Message, MessageId, PulsarClient}
import org.apache.pulsar.client.impl.schema.BytesSchema
import org.apache.pulsar.client.internal.DefaultImplementation
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaInfo

import org.apache.spark.internal.Logging
import org.apache.spark.sql.pulsar.PulsarOptions._
import org.apache.spark.sql.pulsar.topicinternalstats.forward._
import org.apache.spark.sql.types.StructType

/**
 * A Helper class that responsible for:
 *   - getEarliest / Latest / Specific MessageIds
 *   - guarantee message existence using subscription by setup, move and remove
 */
private[pulsar] case class PulsarMetadataReader(
    serviceUrl: String,
    adminUrl: String,
    clientConf: ju.Map[String, Object],
    adminClientConf: ju.Map[String, Object],
    driverGroupIdPrefix: String,
    caseInsensitiveParameters: Map[String, String],
    allowDifferentTopicSchemas: Boolean,
    predefinedSubscription: Option[String])
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
      case off: SpecificPulsarOffset => setupCursorByMid(off, predefinedSubscription)
      case time: SpecificPulsarTime => setupCursorByTime(time, predefinedSubscription)
      case s =>
        throw new UnsupportedOperationException(s"$s shouldn't appear here, a bug occurs.")
    }
  }

  def setupCursorByMid(offset: SpecificPulsarOffset, subscription: Option[String]): Unit = {
    offset.topicOffsets.foreach { case (tp, mid) =>
      val umid = mid.asInstanceOf[UserProvidedMessageId]
      val (subscriptionName, subscriptionPredefined) = extractSubscription(subscription, tp)

      // setup the subscription
      if (!subscriptionPredefined) {
        try {
          admin.topics().createSubscription(tp, subscriptionName, umid.mid)
        } catch {
          case _: PulsarAdminException.ConflictException =>
            // if subscription already exists, log the info and continue to reset cursor
            log.info("Subscription already exists...")
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to setup cursor for ${TopicName.get(tp).toString}",
              e)
        }
      }

      // reset cursor position
      log.info(s"Resetting cursor for $subscriptionName to given offset")
      admin.topics().resetCursor(tp, subscriptionName, umid.mid)
    }
  }

  def setupCursorByTime(time: SpecificPulsarTime, subscription: Option[String]): Unit = {
    time.topicTimes.foreach { case (tp, time) =>
      val msgID = time match {
        case PulsarProvider.EARLIEST_TIME => MessageId.earliest
        case PulsarProvider.LATEST_TIME => MessageId.latest
        case t if t >= 0 => MessageId.latest
        case _ => throw new RuntimeException(s"Invalid starting time for $tp: $time")
      }

      val (subscriptionNames, subscriptionPredefined) = extractSubscription(subscription, tp)

      // setup the subscription
      if (!subscriptionPredefined) {
        try {
          admin.topics().createSubscription(tp, s"$subscriptionNames", msgID)
        } catch {
          case _: PulsarAdminException.ConflictException =>
            // if subscription already exists, log the info and continue to reset cursor
            log.info("subscription already exists...")
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to setup cursor for ${TopicName.get(tp).toString}",
              e)
        }
      }

      // reset cursor position
      log.info(s"Resetting cursor for $subscriptionNames to given timestamp")
      time match {
        case PulsarProvider.EARLIEST_TIME | PulsarProvider.LATEST_TIME =>
          admin.topics().resetCursor(tp, s"$subscriptionNames", msgID)
        case _ =>
          admin.topics().resetCursor(tp, s"$subscriptionNames", time)
      }
    }
  }

  private def extractSubscription(
      subscriptionName: Option[String],
      topicPartition: String): (String, Boolean) = {
    subscriptionName match {
      case None => (s"$driverGroupIdPrefix-$topicPartition", false)
      case Some(subName) => (subName, true)
    }
  }

  def commitCursorToOffset(offset: Map[String, MessageId]): Unit = {
    offset.foreach { case (tp, mid) =>
      try {
        val (subscription, _) = extractSubscription(predefinedSubscription, tp)
        admin.topics().resetCursor(tp, s"$subscription", mid)
      } catch {
        case e: PulsarAdminException if e.getStatusCode == 404 || e.getStatusCode == 412 =>
          logInfo(s"Cannot commit cursor since the topic $tp has been deleted during execution.")
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
      val (subscriptionName, subscriptionPredefined) =
        extractSubscription(predefinedSubscription, tp)

      // Only delete a subscription if it's not predefined and created by us
      if (!subscriptionPredefined) {
        try {
          admin.topics().deleteSubscription(tp, s"$subscriptionName")
        } catch {
          case e: PulsarAdminException if e.getStatusCode == 404 =>
            logInfo(
              s"Cannot remove cursor since the topic $tp has been deleted during execution.")
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to remove cursor for ${TopicName.get(tp).toString}",
              e)
        }
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
    allowDifferentTopicSchemas match {
      case false =>
        if (topics.size > 0) {
          val schemas = topics.map { tp =>
            getPulsarSchema(tp)
          }
          val sset = schemas.toSet
          if (sset.size != 1) {
            throw new IllegalArgumentException(
              "Topics to read must share identical schema. Consider setting " +
                s"'$AllowDifferentTopicSchemas' to 'false' to read topics with empty " +
                s"schemas instead. We got ${sset.size} distinct " +
                s"schemas:[${sset.mkString(", ")}]")
          } else {
            sset.head
          }
        } else {
          // if no topic exists, and we are getting schema,
          // then auto created topic has schema of None
          SchemaUtils.emptySchemaInfo()
        }
      case true => SchemaUtils.emptySchemaInfo()
    }
  }

  def getPulsarSchema(topic: String): SchemaInfo = {
    try {
      admin.schemas().getSchemaInfo(TopicName.get(topic).toString)
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
      (tp -> {
        val messageId =
          try {
            admin.topics().getLastMessageId(tp)
          } catch {
            case e: PulsarAdminException if e.getStatusCode == 404 =>
              MessageId.earliest
            case e: Throwable =>
              throw new RuntimeException(
                s"Failed to get last messageId for ${TopicName.get(tp).toString}",
                e)
          }
        PulsarSourceUtils.seekableLatestMid(messageId)
      })
    }.toMap)
  }

  def fetchNextOffsetWithMaxEntries(actualOffset: Map[String, MessageId],
                                    numberOfEntries: Long): SpecificPulsarOffset = {
    getTopicPartitions()

    // Collect internal stats for all topics
    val topicStats = topicPartitions.map( topic => {
      topic -> admin.topics().getInternalStats(topic)
    } ).toMap.asJava

    SpecificPulsarOffset(topicPartitions.map { topic =>
      topic -> PulsarSourceUtils.seekableLatestMid {
        // Fetch actual offset for topic
        val topicActualMessageId = actualOffset.getOrElse(topic, MessageId.earliest)
        try {
          // Get the actual ledger
          val actualLedgerId = PulsarSourceUtils.getLedgerId(topicActualMessageId)
          // Get the actual entry ID
          val actualEntryId = PulsarSourceUtils.getEntryId(topicActualMessageId)
          // Get the partition index
          val partitionIndex = PulsarSourceUtils.getPartitionIndex(topicActualMessageId)
          // Cache topic internal stats
          val internalStats = topicStats.get(topic)
          // Calculate the amount of messages we will pull in
          val numberOfEntriesPerTopic = numberOfEntries / topics.size
          // Get a next message ID which respects
          // the maximum number of messages
          val (nextLedgerId, nextEntryId) = TopicInternalStatsUtils.forwardMessageId(
            internalStats,
            actualLedgerId,
            actualEntryId,
            numberOfEntriesPerTopic)
          // Build the next message ID
          val nextMessageId =
            DefaultImplementation
              .getDefaultImplementation
              .newMessageId(nextLedgerId, nextEntryId, partitionIndex)
          // Log state
          val entryCountUntilNextMessageId = TopicInternalStatsUtils.numOfEntriesUntil(
            internalStats, nextLedgerId, nextEntryId)
          val entryCount = internalStats.numberOfEntries
          val progress = f"${entryCountUntilNextMessageId.toFloat / entryCount.toFloat}%1.3f"
          val logMessage = s"Pulsar Connector offset step forward. " +
            s"[$numberOfEntriesPerTopic/$numberOfEntries]" +
            s"${topic.reverse.take(30).reverse} $topicActualMessageId -> " +
            s"$nextMessageId ($entryCountUntilNextMessageId/$entryCount) [$progress]"
          log.debug(logMessage)
          // Return the message ID
          nextMessageId
        } catch {
          case e: PulsarAdminException if e.getStatusCode == 404 =>
            MessageId.earliest
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to get forwarded messageId for ${TopicName.get(topic).toString} " +
                s"(tried to forward ${numberOfEntries} messages " +
                s"starting from `$topicActualMessageId`)", e)
        }

      }
    }.toMap)
  }

  def fetchLatestOffsetForTopic(topic: String): MessageId = {
    val messageId =
      try {
        admin.topics().getLastMessageId(topic)
      } catch {
        case e: PulsarAdminException if e.getStatusCode == 404 =>
          MessageId.earliest
        case e: Throwable =>
          throw new RuntimeException(
            s"Failed to get last messageId for ${TopicName.get(topic).toString}",
            e)
      }
    PulsarSourceUtils.seekableLatestMid(messageId)
  }

  def fetchEarliestOffsets(topics: Seq[String]): Map[String, MessageId] = {
    if (topics.isEmpty) {
      Map.empty[String, MessageId]
    } else {
      topics.map(p => p -> MessageId.earliest).toMap
    }
  }

  private def getTopics(): Unit = {
    val optionalTopics =
      caseInsensitiveParameters.find({ case (key, _) => TopicOptionKeys.contains(key) })
    topics = optionalTopics match {
      case Some((TopicSingle, value)) =>
        TopicName.get(value).toString :: Nil
      case Some((TopicMulti, value)) =>
        value.split(",").map(_.trim).filter(_.nonEmpty).map(TopicName.get(_).toString)
      case Some((TopicPattern, value)) =>
        getTopics(value)
      case None =>
        throw new RuntimeException("Failed to get topics from configurations")
    }
  }

  private def getTopicPartitions(): Seq[String] = {
    getTopics()
    topicPartitions = topics.flatMap { tp =>
      val partNum = admin.topics().getPartitionedTopicMetadata(tp).partitions
      if (partNum == 0) {
        tp :: Nil
      } else {
        (0 until partNum).map(tp + PulsarOptions.PartitionSuffix + _)
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
      admin.topics().getPartitionedTopicList(dest.getNamespace)
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

  def offsetForEachTopic(
      params: Map[String, String],
      defaultOffsets: PulsarOffset,
      optionKey: String): PerTopicOffset = {
    getTopicPartitions()

    val offset = PulsarProvider.getPulsarOffset(params, defaultOffsets, optionKey)
    offset match {
      case LatestOffset =>
        SpecificPulsarOffset(
          topicPartitions.map(tp => (tp, UserProvidedMessageId(MessageId.latest))).toMap)
      case EarliestOffset =>
        SpecificPulsarOffset(
          topicPartitions.map(tp => (tp, UserProvidedMessageId(MessageId.earliest))).toMap)
      case so: SpecificPulsarOffset =>
        val specified: Map[String, MessageId] = so.topicOffsets.map { case (tp, mid) =>
          (tp, UserProvidedMessageId(mid))
        }
        assert(
          specified.keySet.subsetOf(topicPartitions.toSet),
          s"topics designated in startingOffsets/endingOffsets" +
            s" should all appear in $TopicOptionKeys .\n" +
            s"topics: $topicPartitions, topics in offsets: ${specified.keySet}")
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
        SpecificPulsarTime(topicPartitions.map(tp => (tp, ts)).toMap)
      case st: SpecificPulsarTime =>
        val specified: Map[String, Long] = st.topicTimes
        assert(
          specified.keySet.subsetOf(topicPartitions.toSet),
          s"topics designated in $optionKey" +
            s" should all appear in $TopicOptionKeys .\n" +
            s"topics: $topicPartitions, topics in $optionKey: ${specified.keySet}")
        val nonSpecifiedTopics = topicPartitions.toSet -- specified.keySet
        val nonSpecified: Map[String, Long] = nonSpecifiedTopics.map { tp =>
          defaultOffsets match {
            case LatestOffset => (tp, PulsarProvider.LATEST_TIME)
            case EarliestOffset => (tp, PulsarProvider.EARLIEST_TIME)
            case _ => throw new IllegalArgumentException("Defaults should be latest or earliest")
          }
        }.toMap
        SpecificPulsarTime(specified ++ nonSpecified)
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
            s" should all appear in $TopicOptionKeys .\n" +
            s"topics: $topicPartitions, topics in offsets: ${specified.keySet}")
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
      case st: SpecificPulsarTime => fetchCurrentOffsets(st, pollTimeoutMs, reportDataLoss)
      case t => throw new IllegalArgumentException(s"not supported offset type: $t")
    }
  }

  def fetchCurrentOffsets(
      time: SpecificPulsarTime,
      pollTimeoutMs: Int,
      reportDataLoss: String => Unit): Map[String, MessageId] = {

    time.topicTimes.map { case (tp, time) =>
      val actualOffset =
        if (time == PulsarProvider.EARLIEST_TIME) {
          UserProvidedMessageId(MessageId.earliest)
        } else if (time == PulsarProvider.LATEST_TIME) {
          UserProvidedMessageId(
            PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp)))
        } else {
          assert(time > 0, s"time less than 0: $time")
          val reader = client
            .newReader()
            .subscriptionRolePrefix(driverGroupIdPrefix)
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
            reader.close()
            if (msg == null) {
              UserProvidedMessageId(MessageId.earliest)
            } else {
              if (msg.getMessageId == earliestId) {
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

    offset.topicOffsets.map { case (tp, off) =>
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
          PulsarSourceUtils.seekableLatestMid(admin.topics().getLastMessageId(tp)))
      case _ =>
        val reader = client
          .newReader()
          .subscriptionRolePrefix(driverGroupIdPrefix)
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
