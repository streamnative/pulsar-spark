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
package org.apache.spark.sql.pulsar.topicinternalstats.forward

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.pulsar.common.policies.data.{ManagedLedgerInternalStats, PersistentTopicInternalStats}

import org.apache.spark.sql.pulsar.topicinternalstats.forward.TopicInternalStatsUtils._

object TopicInternalStatsUtils {

  def forwardMessageId(stats: PersistentTopicInternalStats,
                       startLedgerId: Long,
                       startEntryId: Long,
                       forwardByEntryCount: Long): (Long, Long) = {
    val ledgers = fixLastLedgerInInternalStat(stats).ledgers.asScala.toList
    if (stats.ledgers.isEmpty || (forwardByEntryCount < 1)) {
      // If there is no ledger info, or there is nothing to forward, stay at current ID
      (startLedgerId, startEntryId)
    } else {
      // Find the start index in the list by its ledger ID
      val startLedgerIndex: Int = stats.ledgers.asScala.find(_.ledgerId == startLedgerId) match {
        // If found, start from there
        case Some(index) => ledgers.indexWhere(_.ledgerId == startLedgerId)
        // If it is not, but the value is -1, start from the beginning
        case None if startLedgerId == -1 => 0
        // In any other case, start from the end
        case _ => ledgers.size - 1
      }

      // Clip the start entry ID withing th start ledger if needed
      val startEntryIndex = Math.min(Math.max(startEntryId, 0), ledgers(startLedgerIndex).entries)

      // Create an iterator over the ledgers list
      val statsIterator =
        new PersistentTopicInternalStatsIterator(stats, startLedgerIndex, startEntryIndex)

      // Advance it forward with the amount of forward steps needed
      val (forwardedLedgerId, forwardedEntryId) = (1L to forwardByEntryCount)
        .map(_ => {statsIterator.next()}).last

      (forwardedLedgerId, forwardedEntryId)
    }
  }

  def numOfEntriesUntil(stats: PersistentTopicInternalStats,
                        ledgerId: Long,
                        entryId: Long): Long = {
    val ledgers = fixLastLedgerInInternalStat(stats).ledgers.asScala
    if (ledgers.isEmpty) {
      0
    } else {
      val ledgersBeforeStartLedger = fixLastLedgerInInternalStat(stats).ledgers
        .asScala
        .filter(_.ledgerId < ledgerId)
      val entriesInLastLedger = if (ledgersBeforeStartLedger.isEmpty) {
        Math.max(entryId, 0)
      } else {
        Math.min(Math.max(entryId, 0), ledgersBeforeStartLedger.last.entries)
      }
      entriesInLastLedger + ledgersBeforeStartLedger.map(_.entries).sum
    }
  }

  def numOfEntriesAfter(stats: PersistentTopicInternalStats,
                        ledgerId: Long,
                        entryId: Long): Long = {
    val ledgers = fixLastLedgerInInternalStat(stats).ledgers.asScala
    if (ledgers.isEmpty) {
      0
    } else {
      val entryCountIncludingCurrentLedger = fixLastLedgerInInternalStat(stats).ledgers
        .asScala
        .filter(_.ledgerId >= ledgerId)
      val entriesInFirstLedger = if (entryCountIncludingCurrentLedger.isEmpty) {
        Math.max(entryId, 0)
      } else {
        Math.min(Math.max(entryId, 0), entryCountIncludingCurrentLedger.last.entries)
      }
      entryCountIncludingCurrentLedger.map(_.entries).sum - entriesInFirstLedger
    }
  }

  def fixLastLedgerInInternalStat(
                stats: PersistentTopicInternalStats): PersistentTopicInternalStats = {
    if (stats.ledgers.isEmpty) {
      stats
    } else {
      val lastLedgerInfo = stats.ledgers.get(stats.ledgers.size() - 1)
      lastLedgerInfo.entries = stats.currentLedgerEntries
      stats.ledgers.set(stats.ledgers.size() - 1, lastLedgerInfo)
      stats
    }
  }

}

class PersistentTopicInternalStatsIterator(stats: PersistentTopicInternalStats,
                                           startLedgerIndex: Int,
                                           startEntryIndex: Long)
  extends Iterator[(Long, Long)] {
  val ledgers = fixLastLedgerInInternalStat(stats).ledgers.asScala.toList
  private var currentLedgerIndex = startLedgerIndex
  private var currentEntryIndex = startEntryIndex

  override def hasNext: Boolean = !isLast
  // If we are pointing to the last element
  private def isLast: Boolean = currentLedgerIndex.equals(ledgers.size - 1) &&
    currentEntryIndex.equals(ledgers.last.entries - 1)

  override def next(): (Long, Long) = {
    // Do not move past last element
    if (hasNext) {
      if (currentEntryIndex < (ledgers(currentLedgerIndex).entries - 1)) {
        // Staying in the current ledger
        currentEntryIndex += 1
      } else {
        // Advancing to the next ledger
        currentLedgerIndex += 1
        currentEntryIndex = 0
      }
    }
    (ledgers(currentLedgerIndex).ledgerId, currentEntryIndex)
  }
}
