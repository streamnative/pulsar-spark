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

class PulsarFailoverIntegrationSuite extends PulsarSourceTest {
  import PulsarOptions._
  import testImplicits._

  test("read from primary with AutoClusterFailover when service.url is omitted") {
    val topic = newTopic()
    sendMessages(topic, (1 to 3).map(_.toString).toArray)

    val pulsar = spark.readStream
      .format("pulsar")
      .option(PulsarFailoverPrimaryServiceUrlDisplayKey, serviceUrl)
      .option(s"${PulsarFailoverSecondaryPrefix}0.serviceUrl", "pulsar://127.0.0.1:1")
      .option(StartingOffsetsOptionKey, "earliest")
      .option(TopicSingle, topic)
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]

    testStream(pulsar)(makeSureGetOffsetCalled, CheckAnswer("1", "2", "3"))
  }
}
