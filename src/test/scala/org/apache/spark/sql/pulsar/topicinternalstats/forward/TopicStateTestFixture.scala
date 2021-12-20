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

import java.util

import org.apache.pulsar.common.policies.data.ManagedLedgerInternalStats.LedgerInfo
import org.apache.pulsar.common.policies.data.PersistentTopicInternalStats

object TopicStateFixture {

  def createTopicState(topicInternalStats: PersistentTopicInternalStats,
                       ledgerId: Long,
                       entryId: Long): TopicState = {
    TopicState(topicInternalStats, ledgerId, entryId)
  }

  def createPersistentTopicInternalStat(ledgers: LedgerInfo*): PersistentTopicInternalStats = {
    val result = new PersistentTopicInternalStats()

    result.currentLedgerEntries = if (ledgers.isEmpty) {
      0
    } else {
      ledgers.last.entries
    }

    if (!ledgers.isEmpty) {
      // simulating a bug in the Pulsar Admin interface
      // (the last ledger in the list of ledgers has 0
      // as entry count instead of the current entry
      // count)
      val modifiedLastEntryId = ledgers.last
      modifiedLastEntryId.entries = 0
    }
    result.ledgers = util.Arrays.asList(ledgers: _*)
    result
  }

  def createLedgerInfo(ledgerId: Long, entries: Long): LedgerInfo = {
    val result = new LedgerInfo()
    result.ledgerId = ledgerId
    result.entries = entries
    result
  }
}

