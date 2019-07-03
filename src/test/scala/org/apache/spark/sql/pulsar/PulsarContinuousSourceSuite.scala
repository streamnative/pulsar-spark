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

import org.apache.spark.sql.execution.datasources.v2.StreamingDataSourceV2Relation

// trigger test in continuous mode
class PulsarContinuousSourceSuite extends PulsarSourceSuiteBase with PulsarContinuousTest

class PulsarContinuousSourceTopicDeletionSuite extends PulsarContinuousTest {
  import PulsarOptions._
  import testImplicits._

  test("subscribing topic by pattern with topic deletions") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-seems"
    val topic2 = topicPrefix + "-bad"
    createTopic(topic, partitions = 5)
    sendMessages(topic, Array("-1"))
    require(getLatestOffsets(Set(topic)).size === 5)

    val reader = spark
      .readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_PATTERN, s"$topicPrefix-.*")
      .option("failOnDataLoss", "false")

    val pulsar = reader.load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
    val mapped = pulsar.map(v => v.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      Execute { query =>
        deleteTopic(topic)
        createTopic(topic2, partitions = 5)
        eventually(timeout(streamingTimeout)) {
          assert(
            query.lastExecution.logical.collectFirst {
              case StreamingDataSourceV2Relation(_, _, _, r: PulsarContinuousReader) => r
            }.exists { r =>
              // Ensure the new topic is present and the old topic is gone.
              val topic2Parts = (0 until 5).map(p => s"$topic2$PARTITION_SUFFIX$p")
              val knowns = r.knownTopics
              knowns.equals(topic2Parts.toSet)
            },
            s"query never reconfigured to new topic $topic2")
        }
      },
      AddPulsarData(Set(topic2), 4, 5, 6),
      CheckAnswer(2, 3, 4, 5, 6, 7)
    )
  }
}
