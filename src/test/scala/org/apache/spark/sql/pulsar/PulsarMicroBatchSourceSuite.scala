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

import java.util.concurrent.ConcurrentLinkedQueue

import scala.collection.JavaConverters._

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.naming.NamespaceName
import org.apache.pulsar.common.policies.data.RetentionPolicies
import org.apache.spark.SparkException
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.execution.streaming.{StreamExecution, StreamingExecutionRelation}
import org.apache.spark.sql.functions.{col, count, window}
import org.apache.spark.sql.pulsar.PulsarOptions.{ServiceUrlOptionKey, TopicPattern}
import org.apache.spark.sql.streaming.StreamingQueryProgress
import org.apache.spark.sql.streaming.Trigger.ProcessingTime
import org.apache.spark.sql.streaming.{StreamingQuery, StreamingQueryProgress, Trigger}
import org.apache.spark.sql.{ForeachWriter, Row}
import org.apache.spark.util.Utils
import org.scalatest.concurrent.Eventually.{eventually, timeout}
import org.scalatest.time.SpanSugar._

import java.util.UUID
import java.util.concurrent.ConcurrentLinkedQueue

class PulsarSourceTTLSuite extends PulsarSourceTest {
  import PulsarOptions._
  private val MESSAGE_TTL_SECONDS = 5

  override def beforeAll(): Unit = {
    brokerConfigs.put("messageExpiryCheckIntervalInMinutes", "1")
    super.beforeAll()
  }

  private def verifyOutputDataFromTopic(outputTopic: String, expectedRange: Range): Unit = {
    val outputData = spark.read
      .format("pulsar")
      .option("service.url", serviceUrl)
      .option("topic", outputTopic)
      .load()
      .select(col("value").cast("STRING"))
    checkAnswer(outputData, expectedRange.map(r => Row(r.toString)))
  }

  test("test data loss with topic deletion across batches") {
    val inputTopic = newTopic()
    val outputTopic = newTopic()
    createNonPartitionedTopic(inputTopic)
    createNonPartitionedTopic(outputTopic)

    var query: StreamingQuery = null
    try {
      withTempPaths(1) { case Seq(checkpointDir) =>
        query = spark.readStream
          .format("pulsar")
          .option("startingOffsets", "earliest")
          .option("service.url", serviceUrl)
          .option("failOnDataLoss", false)
          .option("topic", inputTopic)
          .load()
          .select(col("value").cast("STRING"))
          .writeStream
          .trigger(Trigger.ProcessingTime("5 seconds"))
          .queryName("continuous-data-loss-test")
          .format("pulsar")
          .option("service.url", serviceUrl)
          .option("topic", outputTopic)
          .option("checkpointLocation", checkpointDir.getCanonicalPath)
          .start()

        // Batch 1
        logInfo("Batch 1: Sending initial messages (0-9)")
        val batch1Messages = (0 until 10).map(_.toString).toArray
        sendMessages(inputTopic, batch1Messages, None)
        
        eventually(timeout(3.seconds)) {
          verifyOutputDataFromTopic(outputTopic, 0 until 10)
        }

        // Simulate TTL between batches
        deleteTopic(inputTopic)
        createNonPartitionedTopic(inputTopic)

        // Batch 2
        logInfo("Batch 2: Sending new messages (10-19) after TTL")
        val batch2Messages = (10 until 20).map(_.toString).toArray
        sendMessages(inputTopic, batch2Messages, None)
        eventually(timeout(5.seconds)) {
          // Do not skip the first message of Batch 2 (10). 
          verifyOutputDataFromTopic(outputTopic, 0 until 20)
          assert(query.isActive, "Query should still be active after data loss")
          assert(query.exception.isEmpty, "Query should not have exceptions when failOnDataLoss=false")
        }
      }
    } finally {
      if (query != null) {
        query.stop()
      }
    }
  }
}

class PulsarMicroBatchV1SourceSuite extends PulsarMicroBatchSourceSuiteBase {
  test("V1 Source is used by default") {
    val topic = newTopic()

    val pulsar = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicPattern, s"$topic.*")
      .load()

    testStream(pulsar)(
      makeSureGetOffsetCalled,
      AssertOnQuery { query =>
        query.logicalPlan.collect { case StreamingExecutionRelation(_: PulsarSource, _, _) =>
          true
        }.nonEmpty
      })
  }
}

abstract class PulsarMicroBatchSourceSuiteBase extends PulsarSourceSuiteBase {
  import PulsarOptions._
  import testImplicits._

  test("(de)serialization of initial offsets") {
    val topic = newTopic()
    createNonPartitionedTopic(topic)

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicSingle, topic)

    testStream(reader.load)(makeSureGetOffsetCalled, StopStream, StartStream(), StopStream)
  }

  test("input row metrics") {
    val topic = newTopic()
    createTopic(topic, 12)
    sendMessages(topic, Array("-1"))

    val pulsar = spark.readStream
      .format("pulsar")
      .option(TopicSingle, topic)
      .option(ServiceUrlOptionKey, serviceUrl)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    def checkSourceMetrics(
        progresses: Array[StreamingQueryProgress],
        numInputRows: Long): Boolean = {
      val sourceMetrics = progresses.map(_.sources.head.metrics)
      sourceMetrics.map(_.get("numInputRows").toLong).sum == numInputRows &&
        sourceMetrics.map(_.get("numInputBytes").toLong).sum >= numInputRows &&
        progresses.map(_.numInputRows).sum == numInputRows
    }

    val mapped = pulsar.map(kv => kv._2.toInt + 1)
    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        checkSourceMetrics(query.recentProgress, 3)
      },
      AddPulsarData(Set(topic), 4, 5),
      CheckAnswer(2, 3, 4, 5, 6),
      AssertOnQuery { query =>
        checkSourceMetrics(query.recentProgress, 5)
      },
      StopStream,
      StartStream(trigger = ProcessingTime(1)),
      AddPulsarData(Set(topic), 6),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      AssertOnQuery { query =>
        checkSourceMetrics(query.recentProgress, 1)
      },
    )
  }

  test("subscribing topic by pattern with topic deletions") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-seems"
    val topic2 = topicPrefix + "-bad"
    createNonPartitionedTopic(topic)
    sendMessages(topic, Array("-1"))
    require(getLatestOffsets(Set(topic)).size === 1)

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicPattern, s"$topicPrefix-.*")
      .option("failOnDataLoss", "false")

    val pulsar = reader
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
    val mapped = pulsar.map(v => v.toInt + 1)

    testStream(mapped)(
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      Assert {
        deleteTopic(topic)
        createNonPartitionedTopic(topic2)
        true
      },
      AddPulsarData(Set(topic2), 4, 5, 6),
      CheckAnswer(2, 3, 4, 5, 6, 7)
    )
  }

  test("subscribe topic by pattern with topic recreation between batches") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-good"
    val topic2 = topicPrefix + "-bad"
    createNonPartitionedTopic(topic)
    sendMessages(topic, Array("1", "3"))
    createNonPartitionedTopic(topic2)
    sendMessages(topic2, Array("2", "4"))

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicPattern, s"$topicPrefix-.*")
      .option("failOnDataLoss", "true")
      .option("startingOffsets", "earliest")

    val ds = reader
      .load()
      .selectExpr("CAST(value AS STRING)")
      .as[String]
      .map(v => v.toInt)

    testStream(ds)(
      StartStream(),
      AssertOnQuery { q =>
        q.processAllAvailable()
        true
      },
      CheckAnswer(1, 2, 3, 4),
      // Restart the stream in this test to make the test stable. When recreating a topic when a
      // consumer is alive, it may not be able to see the recreated topic even if a fresh consumer
      // has seen it.
      StopStream,
      // Recreate `topic2` and wait until it's available
      TopicRecreation(topic2),
      StartStream(),
      ExpectFailure[SparkException](e => {
        assert(e.getCause.getMessage.contains("Potential Data Loss"))
      })
    )
  }

  case class TopicRecreation(topic2: String) extends ExternalAction {
    override def runAction(): Unit = {
      deleteTopic(topic2)
      createNonPartitionedTopic(topic2)
      val mid = sendMessages(topic2, Array("6", "7", "8")).last._2
      // waitUntilOffsetAppears(topic2, mid)
    }
  }

  test("PulsarSource with watermark") {
    val now = System.currentTimeMillis()
    val topic = newTopic()
    sendMessages(topic, Array(1).map(_.toString))

    val pulsar = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(StartingOffsetsOptionKey, "earliest")
      .option(TopicSingle, topic)
      .load()

    val windowedAggregation = pulsar
      .withWatermark("__publishTime", "10 seconds")
      .groupBy(window($"__publishTime", "5 seconds") as 'window)
      .agg(count("*") as 'count)
      .select($"window".getField("start") as 'window, $"count")

    val query = windowedAggregation.writeStream
      .format("memory")
      .outputMode("complete")
      .queryName("pulsarWatermark")
      .start()
    query.processAllAvailable()
    val rows = spark.table("pulsarWatermark").collect()
    assert(rows.length === 1, s"Unexpected results: ${rows.toList}")
    val row = rows(0)
    // We cannot check the exact window start time as it depands on the time that messages were
    // inserted by the producer. So here we just use a low bound to make sure the internal
    // conversion works.
    assert(
      row.getAs[java.sql.Timestamp]("window").getTime >= now - 5 * 1000,
      s"Unexpected results: $row")
    assert(row.getAs[Int]("count") === 1, s"Unexpected results: $row")
    query.stop()
  }

  test("delete a topic when a Spark job is running") {
    PulsarSourceSuite.collectedData.clear()

    val topic = newTopic()
    sendMessages(topic, (1 to 10).map(_.toString).toArray)

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicSingle, topic)
      .option(StartingOffsetsOptionKey, "earliest")
      .option(PollTimeoutMS, "1000")
      .option("failOnDataLoss", "false")
    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    // The following ForeachWriter will delete the topic before fetching data from Pulsar
    // in executors.

    val adminu: String = adminUrl
    val query = pulsar
      .map(kv => kv._2.toInt)
      .writeStream
      .foreach(new ForeachWriter[Int] {
        override def open(partitionId: Long, version: Long): Boolean = {
          Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminu).build()) { admin =>
            admin.topics().delete(topic, true)
          }
          true
        }

        override def process(value: Int): Unit = {
          PulsarSourceSuite.collectedData.add(value)
        }

        override def close(errorOrNull: Throwable): Unit = {}
      })
      .start()
    query.processAllAvailable()
    query.stop()
    // `failOnDataLoss` is `false`, we should not fail the query
    assert(query.exception.isEmpty)
  }

  test("ensure stream-stream self-join generates only one offset in log and correct metrics") {
    val topic = newTopic()

    val pulsar = spark.readStream
      .format("pulsar")
      .option(TopicSingle, topic)
      .option(ServiceUrlOptionKey, serviceUrl)
      .load()

    val values = pulsar
      .selectExpr(
        "CAST(CAST(value AS STRING) AS INT) AS value",
        "CAST(CAST(value AS STRING) AS INT) % 5 AS key")

    val join = values.join(values, "key")

    testStream(join)(
      AddPulsarData(Set(topic), 1, 2),
      CheckAnswer((1, 1, 1), (2, 2, 2)),
      AddPulsarData(Set(topic), 6, 3),
      CheckAnswer((1, 1, 1), (2, 2, 2), (3, 3, 3), (1, 6, 1), (1, 1, 6), (1, 6, 6)),
      AssertOnQuery { q =>
        assert(q.availableOffsets.iterator.size == 1)
        assert(q.recentProgress.map(_.numInputRows).sum == 4)
        true
      }
    )
  }
}

object PulsarSourceSuite {
  val collectedData = new ConcurrentLinkedQueue[Any]()
}
