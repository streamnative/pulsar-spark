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

import TopicStateFixture.{createLedgerInfo, _}
import org.apache.spark.SparkFunSuite

class LargeFirstForwardStrategySuite extends SparkFunSuite {

  test("forward empty topics") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(),
        0, 0
      ))
    val testForwarder = new LargeFirstForwardStrategy(10, 0)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 1)
    assert(result("topic1") == 0)
  }

  test("forward a single topic with a single ledger") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200)
        ),
        0, 0
      ))
    val testForwarder = new LargeFirstForwardStrategy(10, 0)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 1)
    assert(result("topic1") == 10)
  }

  test("forward a single topic with multiple ledgers") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200),
          createLedgerInfo(2000, 200)
        ),
        0, 0
      ))
    val testForwarder = new LargeFirstForwardStrategy(350, 0)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 1)
    assert(result("topic1") == 350)
  }

  test("forward a single topic with the biggest backlog") {
    val fakeState = Map(
      "topic1" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200),
        ),
        0, 0
      ),
      "topic2" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 400),
        ),
        0, 0
      ),
      "topic3" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 600),
        ),
        0, 0
      ))
    val testForwarder = new LargeFirstForwardStrategy(15, 0)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 3)
    assert(result("topic3") == 15)
    assert(result("topic2") == 0)
    assert(result("topic1") == 0)
  }

  test("forward multiple topics if the backlog is small enough") {
    val fakeState = Map(
      "topic1" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 20),
        ),
        0, 0
      ),
      "topic2" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 40),
        ),
        0, 0
      ),
      "topic3" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 60),
        ),
        0, 0
      ))
    val testForwarder = new LargeFirstForwardStrategy(100, 0)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 3)
    assert(result("topic3") == 60)
    assert(result("topic2") == 40)
    assert(result("topic1") == 0)
  }

  test("forward by additional entries regardless of backlog size") {
    val maxEntries = 130
    val additionalEntries = 10
    val topic1Backlog = 80
    val topic2Backlog = 60
    val topic3Backlog = 40
    val fakeState = Map(
      "topic1" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, topic1Backlog),
        ),
        0, 0
      ),
      "topic2" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, topic2Backlog),
        ),
        0, 0
      ),
      "topic3" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, topic3Backlog),
        ),
        0, 0
      ))
    val testForwarder = new LargeFirstForwardStrategy(maxEntries, additionalEntries)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 3)

    assert(result("topic1") >= additionalEntries)
    assert(result("topic2") >= additionalEntries)
    assert(result("topic3") == additionalEntries)

  }

  test("additional entries to forward has a higher precedence than max allowed entries") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(),
        0, 0
      ))

    val testForwarder = new LargeFirstForwardStrategy(10, 20)
    val result = testForwarder.forward(fakeState)

    assert(result("topic1") == 20)
  }

  test("forward from the middle of the first topic ledger") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200)
        ),
        1000, 20
      ))
    val testForwarder = new LargeFirstForwardStrategy(80, 0)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 1)
    assert(result("topic1") == 80)
  }

  test("forward from the middle of the last topic ledger") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200),
          createLedgerInfo(2000, 200),
          createLedgerInfo(3000, 200)
        ),
        3000, 20
      ))
    val testForwarder = new LargeFirstForwardStrategy(80, 0)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 1)
    assert(result("topic1") == 80)
  }

}

