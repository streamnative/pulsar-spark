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
import java.util.concurrent.TimeUnit
import java.util.regex.Pattern

import scala.collection.mutable
import scala.language.postfixOps
import scala.util.control.NonFatal

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{MessageId, PulsarClient}
import org.apache.pulsar.client.impl.{MessageIdImpl, PulsarClientImpl}
import org.apache.pulsar.client.impl.schema.BytesSchema
import org.apache.pulsar.client.internal.DefaultImplementation
import org.apache.pulsar.common.api.proto.CommandGetTopicsOfNamespace
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.SchemaInfo
import org.apache.pulsar.shade.com.google.common.util.concurrent.Uninterruptibles

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.streaming
import org.apache.spark.sql.connector.read.streaming.{ReadAllAvailable, ReadLimit}
import org.apache.spark.sql.pulsar.PulsarOptions._
import org.apache.spark.sql.pulsar.PulsarSourceUtils.{getEntryId, getLedgerId}
import org.apache.spark.sql.pulsar.SpecificPulsarOffset.getTopicOffsets
import org.apache.spark.sql.types.StructType

/**
 * A Helper class that is responsible for interacting with Pulsar to conduct subscription
 * management, cursor management, schema and topic metadata lookup etc.
 */
private[pulsar] case class PulsarHelper(
    serviceUrl: String,
    adminUrl: Option[String],
    clientConf: ju.Map[String, Object],
    driverGroupIdPrefix: String,
    caseInsensitiveParameters: Map[String, String],
    allowDifferentTopicSchemas: Boolean,
    predefinedSubscription: Option[String])
    extends Closeable
    with Logging {

  import scala.collection.JavaConverters._

  protected var client: PulsarClientImpl = CachedPulsarClient.getOrCreate(clientConf)

  private var topics: Seq[String] = _
  private var topicPartitions: Seq[String] = _

  private lazy val pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(adminUrl.get).build()

  override def close(): Unit = {
    // do nothing
  }

  def setupCursor(startingPos: PerTopicOffset): Unit = {
    startingPos match {
      case off: SpecificPulsarOffset => setupCursorByMid(off, predefinedSubscription)
      case time: SpecificPulsarTime => setupCursorByTime(time, predefinedSubscription)
      case s =>
        throw new UnsupportedOperationException(s"$s shouldn't appear here, a bug occurs.")
    }
  }

  private def setupCursorByMid(
      offset: SpecificPulsarOffset,
      subscription: Option[String]): Unit = {
    offset.topicOffsets.foreach { case (tp, mid) =>
      val umid = mid.asInstanceOf[UserProvidedMessageId]
      val (subscriptionName, _) = extractSubscription(subscription, tp)

      // establish connection and setup the subscription if needed
      val consumer = CachedConsumer.getOrCreate(tp, subscriptionName, client)

      // reset cursor position
      log.info(s"Resetting cursor for $subscriptionName to given offset")
      consumer.seek(umid.mid)
    }
  }

  private def setupCursorByTime(time: SpecificPulsarTime, subscription: Option[String]): Unit = {
    time.topicTimes.foreach { case (tp, time) =>
      val msgID = time match {
        case PulsarProvider.EARLIEST_TIME => MessageId.earliest
        case PulsarProvider.LATEST_TIME => MessageId.latest
        case t if t >= 0 => MessageId.latest
        case _ => throw new RuntimeException(s"Invalid starting time for $tp: $time")
      }

      val (subscriptionNames, _) = extractSubscription(subscription, tp)

      // establish connection and setup the subscription if needed
      val consumer = CachedConsumer.getOrCreate(tp, subscriptionNames, client)

      // reset cursor position
      log.info(s"Resetting cursor for $subscriptionNames to given timestamp")
      time match {
        case PulsarProvider.EARLIEST_TIME | PulsarProvider.LATEST_TIME =>
          consumer.seek(msgID)
        case _ =>
          consumer.seek(time)
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
        CachedConsumer.getOrCreate(tp, subscription, client).seek(mid)
      } catch {
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
          CachedConsumer.getOrCreate(tp, subscriptionName, client).unsubscribe()
        } catch {
          case e: Throwable =>
            throw new RuntimeException(
              s"Failed to remove cursor for ${TopicName.get(tp).toString}",
              e)
        }
      }
    }
  }

  def getAndCheckCompatible(schema: Option[StructType]): StructType = {
    val si = getPulsarSchema
    val inferredSchema = SchemaUtils.pulsarSourceSchema(si)
    require(
      schema.isEmpty || inferredSchema == schema.get,
      "The Schema of Pulsar source and provided doesn't match")
    inferredSchema
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

  private def getPulsarSchema(topic: String): SchemaInfo = {
    try {
      client.getSchema(topic).get().get()
    } catch {
      case e: NoSuchElementException => BytesSchema.of().getSchemaInfo
      case e: Throwable =>
        throw new RuntimeException(
          s"Failed to get schema information for ${TopicName.get(topic).toString}",
          e)
    }
  }

  def fetchLatestOffsets(): SpecificPulsarOffset = {
    getTopicPartitions
    SpecificPulsarOffset(topicPartitions.map { tp =>
      (tp -> fetchLatestOffsetForTopic(tp))
    }.toMap)
  }

  def latestOffsets(startingOffset: streaming.Offset,
                    totalReadLimit: Long): SpecificPulsarOffset = {
    // implement helper inside PulsarHelper in order to use getTopicPartitions
    val topicPartitions = getTopicPartitions
    // add new partitions from PulsarAdmin, set to earliest entry and ledger id based on limit
    // start a reader, get to the earliest offset for new topic partitions
    val existingStartOffsets = if (startingOffset != null) {
      getTopicOffsets(startingOffset.asInstanceOf[org.apache.spark.sql.execution.streaming.Offset])
    } else {
      Map[String, MessageId]()
    }
    val newTopics = topicPartitions.toSet.diff(existingStartOffsets.keySet)
    val startPartitionOffsets = existingStartOffsets ++ newTopics.map(topicPartition
    => topicPartition -> fetchLatestOffsetForTopic(topicPartition))
    val offsets = mutable.Map[String, MessageId]()
    offsets ++= startPartitionOffsets
    val numPartitions = startPartitionOffsets.size
    // move all topic partition logic to helper function
    startPartitionOffsets.keys.foreach { topicPartition =>
      val readLimit = totalReadLimit / numPartitions
      val startMessageId = startPartitionOffsets.apply(topicPartition)
      offsets += (topicPartition -> latestOffsetForTopic(topicPartition, startMessageId, readLimit))
    }
    SpecificPulsarOffset(offsets.toMap)
  }

  def fetchLatestOffsetForTopic(topic: String): MessageId = {
    val messageId =
      try {
        getLastMessageId(topic)
      } catch {
        case e: Throwable =>
          throw new RuntimeException(
            s"Failed to get last messageId for ${TopicName.get(topic).toString}",
            e)
      }
    PulsarSourceUtils.seekableLatestMid(messageId)
  }

  def latestOffsetForTopic(topicPartition: String,
                           startMessageId: MessageId,
                           readLimit: Long): MessageId = {
    val startLedgerId = getLedgerId(startMessageId)
    val startEntryId = getEntryId(startMessageId)
    val stats = pulsarAdmin.topics.getInternalStats(topicPartition)
    val ledgers = pulsarAdmin.topics.getInternalStats(topicPartition).ledgers.
      asScala.filter(_.ledgerId >= startLedgerId).sortBy(_.ledgerId)
    // The last ledger of the ledgers list doesn't have .size or .entries
    // properly populated, and the corresponding info is in currentLedgerSize
    // and currentLedgerEntries
    if (ledgers.nonEmpty) {
      ledgers.last.size = stats.currentLedgerSize
      ledgers.last.entries = stats.currentLedgerEntries
    }
    var messageId = startMessageId
    var readLimitLeft = readLimit
    ledgers.filter(_.entries != 0).sortBy(_.ledgerId).foreach { ledger =>
      val avgBytesPerEntries = ledger.size / ledger.entries
      // approximation of bytes left in ledger to deal with case
      // where we are at the middle of the ledger
      val bytesLeftInLedger = if (ledger.ledgerId == startLedgerId) {
        avgBytesPerEntries * (ledger.entries - startEntryId - 1)
      } else {
        ledger.size
      }
      if (readLimitLeft > bytesLeftInLedger) {
        readLimitLeft -= bytesLeftInLedger
        messageId = DefaultImplementation
          .getDefaultImplementation
          .newMessageId(ledger.ledgerId, ledger.entries - 1, -1)
      } else {
        val numEntriesToRead = Math.max(1, readLimit / avgBytesPerEntries)
        val lastEntryId = if (ledger.ledgerId != startLedgerId) {
          numEntriesToRead - 1
        } else {
          startEntryId + numEntriesToRead
        }
        val lastEntryRead = Math.min(ledger.entries - 1, lastEntryId)
        messageId = DefaultImplementation
          .getDefaultImplementation
          .newMessageId(ledger.ledgerId, lastEntryRead, -1)
        readLimitLeft = 0
      }
    }
    messageId
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

    waitForTopicIfNeeded()
  }

  private def getTopicPartitions: Seq[String] = {
    getTopics()
    topicPartitions = topics.flatMap { tp =>
      client.getPartitionsForTopic(tp).get().asScala.map(_.toString)
    }
    topicPartitions
  }

  private def getTopics(topicsPattern: String): Seq[String] = {
    val dest = TopicName.get(topicsPattern)
    val allTopics: ju.List[String] = client.getLookup
      .getTopicsUnderNamespace(dest.getNamespaceObject, CommandGetTopicsOfNamespace.Mode.ALL)
      .get()

    val allNonPartitionedTopics: ju.List[String] = allTopics.asScala
      .filter(t => !TopicName.get(t).isPartitioned)
      .asJava
    val nonPartitionedMatch = topicsPatternFilter(allNonPartitionedTopics, dest.toString)

    val allPartitionedTopics: ju.List[String] = allTopics.asScala
      .filter(t => TopicName.get(t).isPartitioned)
      .map(TopicName.get(_).getPartitionedTopicName) // trim partition suffix
      .toSet // deduplicate topics
      .toSeq
      .asJava
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

  private def waitForTopicIfNeeded(): Unit = {
    if (caseInsensitiveParameters.getOrElse(WaitingForNonExistedTopic, "false").toBoolean) {
      // This method will wait the desired topics until it's created.

      val waitList = mutable.ListBuffer(topics: _*)
      while (waitList.nonEmpty) {
        val topic = waitList.head
        try {
          client.getPartitionedTopicMetadata(topic).get()
          waitList -= topic
        } catch {
          case NonFatal(_) =>
            logInfo(s"The desired $topic doesn't existed, wait for 5 seconds.")
            Uninterruptibles.sleepUninterruptibly(5, TimeUnit.SECONDS)
        }
      }
    }
  }

  def offsetForEachTopic(
      params: Map[String, String],
      defaultOffsets: PulsarOffset,
      optionKey: String): PerTopicOffset = {
    getTopicPartitions

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

    getTopicPartitions
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
      case so: SpecificPulsarOffset => fetchCurrentOffsets(so, reportDataLoss)
      case st: SpecificPulsarTime => fetchCurrentOffsets(st, reportDataLoss)
      case t => throw new IllegalArgumentException(s"not supported offset type: $t")
    }
  }

  private def fetchCurrentOffsets(
      offset: SpecificPulsarOffset,
      reportDataLoss: String => Unit): Map[String, MessageId] = {

    offset.topicOffsets.map { case (tp, off) =>
      val actualOffset = off match {
        case MessageId.earliest =>
          UserProvidedMessageId(off)
        case MessageId.latest =>
          UserProvidedMessageId(PulsarSourceUtils.seekableLatestMid(getLastMessageId(tp)))
        case _: MessageId =>
          getUserProvidedMessageId(tp, None, Some(off))
        case _ =>
          throw new IllegalArgumentException(s"not supported offset type: $off")
      }
      (tp, actualOffset)
    }
  }

  private def fetchCurrentOffsets(
      time: SpecificPulsarTime,
      reportDataLoss: String => Unit): Map[String, MessageId] = {

    time.topicTimes.map { case (tp, time) =>
      val actualOffset = time match {
        case PulsarProvider.EARLIEST_TIME =>
          UserProvidedMessageId(MessageId.earliest)
        case PulsarProvider.LATEST_TIME =>
          UserProvidedMessageId(PulsarSourceUtils.seekableLatestMid(getLastMessageId(tp)))
        case _ =>
          assert(time > 0, s"time less than 0: $time")
          getUserProvidedMessageId(tp, Some(time), None)
      }
      (tp, actualOffset)
    }
  }

  private def getUserProvidedMessageId(
      topic: String,
      time: Option[Long],
      offset: Option[MessageId]): MessageIdImpl = {
    // seek to specified offset or time and get the actual corresponding message id
    // we don't call consumer.acknowledge() here because the message is not processed yet
    if (time.isDefined || offset.isDefined) {
      val reader = client
        .newReader()
        .subscriptionRolePrefix(driverGroupIdPrefix)
        .topic(topic)
        .startMessageId(offset.getOrElse(MessageId.earliest))
        .startMessageIdInclusive()
        .create()
      (time, offset) match {
        case (None, Some(o)) =>
        case (Some(t), None) => reader.seek(t)
        case _ =>
          throw new IllegalArgumentException(
            s"one of time and offset should be set. time: $time, offset: $offset")
      }
      val message = reader.readNext()
      reader.close()
      if (message != null) UserProvidedMessageId(PulsarSourceUtils.mid2Impl(message.getMessageId))
      else UserProvidedMessageId(MessageId.earliest)
    } else {
      UserProvidedMessageId(MessageId.earliest)
    }
  }

  private def getLastMessageId(topic: String): MessageId = {
    val (subscriptionName, _) = extractSubscription(predefinedSubscription, topic)
    CachedConsumer.getOrCreate(topic, subscriptionName, client).getLastMessageId
  }
}
