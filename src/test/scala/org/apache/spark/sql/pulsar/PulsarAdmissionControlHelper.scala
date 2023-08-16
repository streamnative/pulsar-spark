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


class PulsarAdmissionControlHelper(adminUrl: String) {

  import scala.collection.JavaConverters._

  private lazy val pulsarAdmin = PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()

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
      if (readLimitLeft == 0) {
        return messageId
      }
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
      } else if (readLimitLeft > 0) {
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

}
