package org.apache.spark.sql.pulsar

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.internal.DefaultImplementation
import org.apache.spark.sql.streaming.Trigger.{Once, ProcessingTime}
import org.apache.spark.util.Utils

class PulsarAdmissionControlSuite extends PulsarSourceTest {

  import PulsarOptions._
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  /**
   * Write unit test to create limits, can construct fake ledger statistics
   * Can call latestOffset() directly from the unit test
   *
   * Just need to verify that each microbatch is <= maxBytesPerTrigger (within some threshold)
   * Can send message of specific size in AddPulsarData here
   */

  test("Check last batch where message size is greater than maxBytesPerTrigger") {
    val topic = newTopic()
    sendMessages(topic, Array("-1"))
    require(getLatestOffsets(Set(topic)).size === 1)
    sparkContext.setLogLevel("INFO")
    val pulsar = spark.readStream
      .format("pulsar")
      .option(TopicSingle, topic)
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(AdminUrlOptionKey, adminUrl)
      .option(MaxBytesPerTrigger, 150)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    // Each Int adds 38 bytes to message size, so we expect 3 Ints in each message
    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1000)),
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic), 1, 2, 3),
      CheckLastBatch(2, 3, 4),
      AddPulsarData(Set(topic), 4, 5, 6, 7, 8, 9),
      CheckLastBatch(8, 9, 10),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 9
      }
    )
  }

//  test("latest") {
//    val topic = newTopic()
//    sendMessages(topic, Array("1"))
//
//    val adminu: String = adminUrl
//    val pulsarHelper = new PulsarAdmissionControlHelper(adminu)
//  }
}
