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
package org.apache.spark.sql.pulsar.topicinternalstats.forward

import scala.collection.JavaConverters.asScalaBufferConverter

import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats

object TopicInternalStatsUtils {

  def forwardMessageId(stats: PersistentTopicInternalStats,
                       startLedgerId: Long,
                       startEntryId: Long,
                       forwardByEntryCount: Long): (Long, Long) = {
    val ledgers = fixLastLedgerInInternalStat(stats).ledgers.asScala.toList
    if (ledgers.isEmpty) {
      // If there are no ledger info, stay at current ID
      (startLedgerId, startEntryId)
    } else {
      // Find the start ledger and entry ID
      var actualLedgerIndex = if (ledgers.exists(_.ledgerId == startLedgerId)) {
        ledgers.indexWhere(_.ledgerId == startLedgerId)
      } else if (startLedgerId == -1) {
        0
      } else {
        ledgers.size - 1
      }

      var actualEntryId = Math.min(Math.max(startEntryId, 0), ledgers(actualLedgerIndex).entries)
      var entriesToSkip = forwardByEntryCount

      while (entriesToSkip > 0) {
        val currentLedger = ledgers(actualLedgerIndex)
        val remainingElementsInCurrentLedger = currentLedger.entries - actualEntryId

        if (entriesToSkip <= remainingElementsInCurrentLedger) {
          actualEntryId += entriesToSkip
          entriesToSkip = 0
        } else if ((remainingElementsInCurrentLedger < entriesToSkip)
          && (actualLedgerIndex < (ledgers.size-1))) {
          // Moving onto the next ledger
          entriesToSkip -= remainingElementsInCurrentLedger
          actualLedgerIndex += 1
          actualEntryId = 0
        } else {
          // This is the last ledger
          val entriesInLastLedger = ledgers(actualLedgerIndex).entries
          actualEntryId = Math.min(entriesToSkip + actualEntryId, entriesInLastLedger)
          entriesToSkip = 0
        }
      }

      (ledgers(actualLedgerIndex).ledgerId, actualEntryId)
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
      val boundedEntryId = if (ledgersBeforeStartLedger.isEmpty) {
        Math.max(entryId, 0)
      } else {
        Math.min(Math.max(entryId, 0), ledgersBeforeStartLedger.last.entries)
      }
      boundedEntryId + ledgersBeforeStartLedger.map(_.entries).sum
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
      val boundedEntryId = if (entryCountIncludingCurrentLedger.isEmpty) {
        Math.max(entryId, 0)
      } else {
        Math.min(Math.max(entryId, 0), entryCountIncludingCurrentLedger.last.entries)
      }
      entryCountIncludingCurrentLedger.map(_.entries).sum - boundedEntryId
    }
  }

  private def fixLastLedgerInInternalStat(
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
