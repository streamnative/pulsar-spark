package org.apache.spark.sql.pulsar

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.internal.DefaultImplementation
import org.apache.spark.sql.pulsar.PulsarSourceUtils.{getEntryId, getLedgerId}
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

    // Each Int adds 49 bytes to message size, so we expect 3 Ints in each message
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

  test("Only admit first entry of ledger") {
    val topic = newTopic()
    val messageIds = sendMessages(topic, Array("1", "2", "3"))
    val firstMid = messageIds.head._2
    val firstLedger = getLedgerId(firstMid)
    val firstEntry = getEntryId(firstMid)
    require(getLatestOffsets(Set(topic)).size === 1)
    val admissionControlHelper = new PulsarAdmissionControlHelper(adminUrl)
    val offset = admissionControlHelper.latestOffsetForTopic(topic, MessageId.earliest, 1)
    assert(getLedgerId(offset) == firstLedger && getEntryId(offset) == firstEntry)

  }

  test("Admit entry in the middle of the ledger") {
    val topic = newTopic()
    val messageIds = sendMessages(topic, Array("1", "2", "3"))
    val firstMid = messageIds.head._2
    val secondMid = messageIds.apply(1)._2
    require(getLatestOffsets(Set(topic)).size === 1)
    val admissionControlHelper = new PulsarAdmissionControlHelper(adminUrl)
    val offset = admissionControlHelper.latestOffsetForTopic(topic, firstMid, 1)
    assert(getLedgerId(offset) == getLedgerId(secondMid) && getEntryId(offset) == getEntryId(secondMid))

  }

  test("Admission Control for multiple topics") {
    val topic1 = newTopic()
    val topic2 = newTopic()

    val pulsar = spark.readStream
      .format("pulsar")
      .option(TopicMulti, s"$topic1,$topic2")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(AdminUrlOptionKey, adminUrl)
      .option(MaxBytesPerTrigger, 300)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    // Each Int adds 49 bytes to message size, so we expect 3 Ints in each message
    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1000)),
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic1), 1, 2, 3),
      CheckLastBatch(2, 3, 4),
      AddPulsarData(Set(topic2), 4, 5, 6, 7, 8, 9),
      CheckLastBatch(8, 9, 10),
      AssertOnQuery { query =>
        val recordsRead = query.recentProgress.map(_.numInputRows).sum
        recordsRead == 9
      }
    )
  }

}