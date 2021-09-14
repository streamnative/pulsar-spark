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

import TopicStateFixture._

import org.apache.spark.SparkFunSuite

class LinearForwardStrategySuite extends SparkFunSuite {

  test("forward empty topics") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(),
        0, 0
      ))
    val testForwarder = new LinearForwardStrategy(10)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 1)
    assert(result("topic1") == 10)
  }

  test("forward a single topic with a single ledger") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200)
        ),
        0, 0
      ))
    val testForwarder = new LinearForwardStrategy(10)
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
    val testForwarder = new LinearForwardStrategy(350)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 1)
    assert(result("topic1") == 350)
  }

  test("forward multiple topics with single ledger") {
    val fakeState = Map(
      "topic1" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200),
        ),
        0, 0
      ),
      "topic2" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200),
        ),
        0, 0
      ),
      "topic3" -> createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200),
        ),
        0, 0
      ))
    val testForwarder = new LinearForwardStrategy(15)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 3)
    assert(result("topic1") == 5)
    assert(result("topic2") == 5)
    assert(result("topic3") == 5)
  }

  test("forward from the middle of the first topic ledger") {
    val fakeState = Map( "topic1" ->
      createTopicState(
        createPersistentTopicInternalStat(
          createLedgerInfo(1000, 200)
        ),
        1000, 20
      ))
    val testForwarder = new LinearForwardStrategy(80)
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
    val testForwarder = new LinearForwardStrategy(80)
    val result = testForwarder.forward(fakeState)

    assert(result.size == 1)
    assert(result("topic1") == 80)
  }

}

