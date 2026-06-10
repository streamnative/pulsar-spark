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

import java.util.Locale

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.{PulsarClient, SubscriptionType}

import org.apache.spark.util.Utils

class PulsarSubscriptionTypeSuite extends PulsarSourceTest {
  import PulsarOptions._
  import testImplicits._

  test("invalid subscription type fails at query creation") {
    val ex = intercept[IllegalArgumentException] {
      spark.readStream
        .format("pulsar")
        .option(ServiceUrlOptionKey, serviceUrl)
        .option(TopicSingle, newTopic())
        .option(SubscriptionTypeOptionKey, "round-robin")
        .load()
    }
    assert(ex.getMessage.toLowerCase(Locale.ROOT).contains(SubscriptionTypeOptionKey))
  }

  test("stream with a shared subscription") {
    val topic = newTopic()
    val subscriptionName = "shared-spark-sub"
    sendMessages(topic, (1 to 3).map(_.toString).toArray)

    // Attach an external Shared consumer and keep it connected for the whole test.
    // An Exclusive cursor consumer would fail to join the subscription, so this
    // exercises the connector actually subscribing with the Shared type.
    val client = PulsarClient.builder().serviceUrl(serviceUrl).build()
    val externalConsumer = client
      .newConsumer()
      .topic(topic)
      .subscriptionName(subscriptionName)
      .subscriptionType(SubscriptionType.Shared)
      .subscribe()

    try {
      val pulsar = spark.readStream
        .format("pulsar")
        .option(ServiceUrlOptionKey, serviceUrl)
        .option(TopicSingle, topic)
        .option(PredefinedSubscription, subscriptionName)
        .option(SubscriptionTypeOptionKey, "shared")
        .option(StartingOffsetsOptionKey, "earliest")
        .load()
        .selectExpr("CAST(value AS STRING)")
        .as[String]
        .map(_.toInt)

      testStream(pulsar)(
        makeSureGetOffsetCalled,
        CheckAnswer(1, 2, 3),
        AddPulsarData(Set(topic), 4, 5),
        CheckAnswer(1, 2, 3, 4, 5))

      assert(externalConsumer.isConnected)
      Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
        val subscriptions = admin.topics().getStats(topic).getSubscriptions
        assert(subscriptions.containsKey(subscriptionName))
        assert(subscriptions.get(subscriptionName).getType == "Shared")
      }
    } finally {
      externalConsumer.close()
      client.close()
    }
  }

  test("stream with a key_shared subscription") {
    val topic = newTopic()
    val subscriptionName = "key-shared-spark-sub"
    sendMessages(topic, (1 to 3).map(_.toString).toArray)

    val pulsar = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicSingle, topic)
      .option(PredefinedSubscription, subscriptionName)
      .option(SubscriptionTypeOptionKey, "key_shared")
      .option(StartingOffsetsOptionKey, "earliest")
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(_.toInt)

    testStream(pulsar)(
      makeSureGetOffsetCalled,
      CheckAnswer(1, 2, 3),
      AddPulsarData(Set(topic), 4, 5),
      CheckAnswer(1, 2, 3, 4, 5))

    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      val subscriptions = admin.topics().getStats(topic).getSubscriptions
      assert(subscriptions.containsKey(subscriptionName))
      assert(subscriptions.get(subscriptionName).getType == "Key_Shared")
    }
  }
}
