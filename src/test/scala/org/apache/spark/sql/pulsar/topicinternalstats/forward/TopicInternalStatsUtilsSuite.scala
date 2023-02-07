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

import TopicStateFixture._

import org.apache.spark.SparkFunSuite

class TopicInternalStatsUtilsSuite extends SparkFunSuite {

  test("forward a single entry") {
    val fakeStats = createPersistentTopicInternalStat(createLedgerInfo(1000, 500))
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 0, 0, 1)

    assert(nextLedgerId == 1000)
    assert(nextEntryId == 1)
  }

  test("forward empty ledger") {
    val fakeStats = createPersistentTopicInternalStat()
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 0, 0, 10)

    assert(nextLedgerId == 0)
    assert(nextEntryId == 0)
  }

  test("forward within a single ledger") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 500)
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 1000, 0, 10)

    assert(nextLedgerId == 1000)
    assert(nextEntryId == 10)
  }

  test("forward within a single ledger starting from the middle") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 500)
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 1000, 25, 10)

    assert(nextLedgerId == 1000)
    assert(nextEntryId == 35)
  }

  test("forward to the next ledger") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50)
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 1000, 25, 50)

    assert(nextLedgerId == 2000)
    assert(nextEntryId == 25)
  }

  test("skip over a ledger if needed") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 1000, 25, 100)

    assert(nextLedgerId == 3000)
    assert(nextEntryId == 25)
  }

  test("forward to the end of the topic if too many entries need " +
    "to be skipped with a single ledger") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 1000, 25, 600)

    assert(nextLedgerId == 1000)
    assert(nextEntryId == 49)
  }

  test("forward to the end of the topic if too many entries need " +
    "to be skipped with multiple ledgers") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 1000, 25, 600)

    assert(nextLedgerId == 3000)
    assert(nextEntryId == 49)
  }

  test("forward with zero elements shall give you back what was given") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 2000, 25, 0)

    assert(nextLedgerId == 2000)
    assert(nextEntryId == 25)
  }

  test("forward from beginning of the topic") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, -1, -1, 125)

    assert(nextLedgerId == 3000)
    assert(nextEntryId == 25)
  }

  test("forward from non-existent ledger id shall forward from the last ledger instead") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 6000, 0, 25)

    assert(nextLedgerId == 3000)
    assert(nextEntryId == 25)
  }

  test("forward from non-existent entry id shall forward from end of ledger instead") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, 1000, 250, 25)

    assert(nextLedgerId == 2000)
    assert(nextEntryId == 24)
  }

  test("forwarded entry id shall never be less than current entry id") {
    val startEntryID = 200
    val ledgerID = 1000
    val entriesInLedger = 205
    val forwardByEntries = 50
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(ledgerID, entriesInLedger)
    )
    val (nextLedgerId, nextEntryId) =
      TopicInternalStatsUtils.forwardMessageId(fakeStats, ledgerID, startEntryID, forwardByEntries)
    assert(nextLedgerId == ledgerID)
    assert(nextEntryId > startEntryID)
  }

  test("number of entries until shall work with empty input") {
    val fakeStats = createPersistentTopicInternalStat()
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, -1, -1)

    assert(result == 0)
  }

  test("number of entries until with single ledger") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, 1000, 25)

    assert(result == 25)
  }

  test("number of entries until with multiple ledgers") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, 2000, 25)

    assert(result == 75)
  }

  test("number of entries until beginning of topic") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, -1, -1)

    assert(result == 0)
  }

  test("number of entries until end of topic") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, 3000, 50)

    assert(result == 150)
  }

  test("number of entries until with ledger id below boundary") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, -2, 0)

    assert(result == 0)
  }

  test("number of entries until with entry id below boundary") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, 2000, -2)

    assert(result == 50)

  }

  test("number of entries until with ledger id above boundary") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, 6000, 0)

    assert(result == 150)
  }

  test("number of entries until with entry id above boundary") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesUntil(fakeStats, 2000, 200)

    assert(result == 100)
  }

  test("number of entries after shall work with empty input") {
    val fakeStats = createPersistentTopicInternalStat()
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, -1, -1)

    assert(result == 0)
  }

  test("number of entries after with single ledger") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, 1000, 20)

    assert(result == 30)
  }

  test("number of entries after with multiple ledgers") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, 1000, 20)

    assert(result == 130)
  }

  test("number of entries after beginning of topic") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, -1, -1)

    assert(result == 150)
  }

  test("number of entries after end of topic") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, 3000, 50)

    assert(result == 0)
  }

  test("number of entries after with ledger id below boundary") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, -2, 0)

    assert(result == 150)
  }

  test("number of entries after with entry id below boundary") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, 2000, -2)

    assert(result == 100)
  }

  test("number of entries after with ledger id above boundary") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, 6000, 0)

    assert(result == 0)
  }

  test("number of entries after with entry id above boundary") {
    val fakeStats = createPersistentTopicInternalStat(
      createLedgerInfo(1000, 50),
      createLedgerInfo(2000, 50),
      createLedgerInfo(3000, 50),
    )
    val result =
      TopicInternalStatsUtils.numOfEntriesAfter(fakeStats, 2000, 200)

    assert(result == 50)
  }
}
