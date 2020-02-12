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

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.spark.SparkException
import org.apache.spark.sql.ForeachWriter
import org.apache.spark.sql.execution.streaming.StreamingExecutionRelation
import org.apache.spark.sql.functions.{count, window}
import org.apache.spark.sql.pulsar.PulsarOptions.{ADMIN_URL_OPTION_KEY, SERVICE_URL_OPTION_KEY, TOPIC_PATTERN}
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.util.Utils

class PulsarMicroBatchV1SourceSuite extends PulsarMicroBatchSourceSuiteBase {

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set(
      "spark.sql.streaming.disabledV2MicroBatchReaders",
      classOf[PulsarProvider].getCanonicalName)
  }

  test("V1 Source is used when disabled through SQLConf") {
    val topic = newTopic()

    val pulsar = spark.readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_PATTERN, s"$topic.*")
      .load()

    testStream(pulsar)(
      makeSureGetOffsetCalled,
      AssertOnQuery { query =>
        query.logicalPlan.collect {
          case StreamingExecutionRelation(_: PulsarSource, _) => true
        }.nonEmpty
      }
    )
  }
}

class PulsarMicroBatchV2SourceSuite extends PulsarMicroBatchSourceSuiteBase {

  test("V2 Source is used by default") {
    val topic = newTopic()

    val pulsar = spark.readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_PATTERN, s"$topic.*")
      .load()

    testStream(pulsar)(
      makeSureGetOffsetCalled,
      AssertOnQuery { query =>
        query.logicalPlan.collect {
          case StreamingExecutionRelation(_: PulsarMicroBatchReader, _) => true
        }.nonEmpty
      }
    )
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
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)

    testStream(reader.load)(makeSureGetOffsetCalled, StopStream, StartStream(), StopStream)
  }

  test("input row metrics") {
    val topic = newTopic()
    sendMessages(topic, Array("-1"))
    require(getLatestOffsets(Set(topic)).size === 1)

    val pulsar = spark.readStream
      .format("pulsar")
      .option(TOPIC_SINGLE, topic)
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val mapped = pulsar.map(kv => kv._2.toInt + 1)
    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1)),
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic), 1, 2, 3),
      CheckAnswer(2, 3, 4),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 3
      }
    )
  }

  test("subscribing topic by pattern with topic deletions") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-seems"
    val topic2 = topicPrefix + "-bad"
    createTopic(topic, partitions = 5)
    sendMessages(topic, Array("-1"))
    require(getLatestOffsets(Set(topic)).size === 5)

    val reader = spark.readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_PATTERN, s"$topicPrefix-.*")
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
        createTopic(topic2, partitions = 5)
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
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_PATTERN, s"$topicPrefix-.*")
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
      WithOffsetSync(topic2),
      StartStream(),
      ExpectFailure[SparkException](e => {
        assert(e.getMessage.contains("Potential Data Loss"))
      })
    )
  }

  case class WithOffsetSync(topic2: String) extends ExternalAction {
    override def runAction(): Unit = {
      deleteTopic(topic2)
      createNonPartitionedTopic(topic2)
      val mid = sendMessages(topic2, Array("6")).head._2
      waitUntilOffsetAppears(topic2, mid)
    }
  }

  test("PulsarSource with watermark") {
    val now = System.currentTimeMillis()
    val topic = newTopic()
    sendMessages(topic, Array(1).map(_.toString))

    val pulsar = spark.readStream
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(TOPIC_SINGLE, topic)
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
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(POLL_TIMEOUT_MS, "1000")
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
      .option(TOPIC_SINGLE, topic)
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
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
