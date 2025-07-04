package org.apache.spark.sql.pulsar

import java.{util => ju}

import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.client.internal.DefaultImplementation
import org.apache.spark.sql.pulsar.PulsarSourceUtils.{getEntryId, getLedgerId}
import org.apache.spark.sql.streaming.Trigger.{Once, ProcessingTime}
import org.apache.spark.util.Utils

class PulsarAdmissionControlSuite extends PulsarSourceTest {

  import PulsarOptions._
  import testImplicits._

  private val maxEntriesPerLedger = "managedLedgerMaxEntriesPerLedger"
  private val ledgerRolloverTime = "managedLedgerMinLedgerRolloverTimeMinutes"
  private val approxSizeOfInt = 50

  override def beforeAll(): Unit = {
    brokerConfigs.put(maxEntriesPerLedger, "3")
    brokerConfigs.put(ledgerRolloverTime, "0")
    super.beforeAll()
  }

  override def afterAll(): Unit = {
    super.afterAll()
  }

  test("Only admit first entry of ledger") {
    val topic = newTopic()
    val messageIds = sendMessages(topic, Array("1", "2", "3"))
    val firstMid = messageIds.head._2
    val firstLedger = getLedgerId(firstMid)
    val firstEntry = getEntryId(firstMid)
    require(getLatestOffsets(Set(topic)).size === 1)
    val admissionControlHelper = new PulsarAdmissionControlHelper(adminUrl, new ju.HashMap[String, Object]())
    val offset = admissionControlHelper.latestOffsetForTopicPartition(topic, MessageId.earliest, approxSizeOfInt)
    assert(getLedgerId(offset) == firstLedger && getEntryId(offset) == firstEntry)
  }

  test("setting invalid config PulsarAdmin") {
    val conf = new ju.HashMap[String, Object]()
    conf.put("INVALID", "INVALID")

    val topic = newTopic()
    // Need to call latestOffsetForTopicPartition so the helper instantiates
    // the admin
    val admissionControlHelper = new PulsarAdmissionControlHelper(adminUrl, conf)
    val e = intercept[RuntimeException] {
      admissionControlHelper.latestOffsetForTopicPartition(topic, MessageId.earliest, approxSizeOfInt)
    }
    assert(e.getMessage.contains("Failed to load config into existing configuration data"))
  }

  test("Admit entry in the middle of the ledger") {
    val topic = newTopic()
    val messageIds = sendMessages(topic, Array("1", "2", "3"))
    val firstMid = messageIds.head._2
    val secondMid = messageIds.apply(1)._2
    require(getLatestOffsets(Set(topic)).size === 1)
    val admissionControlHelper = new PulsarAdmissionControlHelper(adminUrl, new ju.HashMap[String, Object]())
    val offset = admissionControlHelper.latestOffsetForTopicPartition(topic, firstMid, approxSizeOfInt)
    assert(getLedgerId(offset) == getLedgerId(secondMid) && getEntryId(offset) == getEntryId(secondMid))

  }

  test("Check last batch where message size is greater than maxBytesPerTrigger") {
    val topic = newTopic()
    sendMessages(topic, Array("-1"))
    require(getLatestOffsets(Set(topic)).size === 1)

    val pulsar = spark.readStream
      .format("pulsar")
      .option(TopicSingle, topic)
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(AdminUrlOptionKey, adminUrl)
      .option(FailOnDataLossOptionKey, "true")
      .option(MaxBytesPerTrigger, approxSizeOfInt * 3)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1000)),
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic), 1, 2, 3),
      AddPulsarData(Set(topic), 4, 5, 6, 7, 8, 9),
      AssertOnQuery { query =>
        query.recentProgress.map(microBatch =>
          microBatch.numInputRows <= 4
        ).forall(_ == true)
      }
    )
  }

  test("Admission Control for multiple topics") {
    val topic1 = newTopic()
    val topic2 = newTopic()

    val pulsar = spark.readStream
      .format("pulsar")
      .option(TopicMulti, s"$topic1,$topic2")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(AdminUrlOptionKey, adminUrl)
      .option(FailOnDataLossOptionKey, "true")
      .option(MaxBytesPerTrigger, approxSizeOfInt * 6)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1000)),
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic1), 1, 2, 3),
      AddPulsarData(Set(topic2), 4, 5, 6, 7, 8, 9),
      AssertOnQuery { query =>
        query.recentProgress.map(microBatch =>
          microBatch.numInputRows <= 4
        ).forall(_ == true)
      }
    )
  }

  test("Admission Control for concurrent topic writes") {
    val topic1 = newTopic()
    val topic2 = newTopic()

    val pulsar = spark.readStream
      .format("pulsar")
      .option(TopicMulti, s"$topic1,$topic2")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(AdminUrlOptionKey, adminUrl)
      .option(FailOnDataLossOptionKey, "true")
      .option(MaxBytesPerTrigger, approxSizeOfInt * 6)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]

    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1000)),
      makeSureGetOffsetCalled,
      AddPulsarData(Set(topic1, topic2), 1, 2, 3),
      AddPulsarData(Set(topic1, topic2), 4, 5, 6, 7, 8, 9),
      AssertOnQuery { query =>
        query.recentProgress.map(microBatch =>
          microBatch.numInputRows <= 4
        ).forall(_ == true)
      }
    )
  }

  test("Set invalid configuration for PulsarAdmin in stream options") {
    val topic = newTopic()
    sendMessages(topic, Array("-1"))
    require(getLatestOffsets(Set(topic)).size === 1)

    val e = intercept[IllegalArgumentException] {
      spark.readStream
        .format("pulsar")
        .option(TopicSingle, topic)
        .option(ServiceUrlOptionKey, serviceUrl)
        .option(AdminUrlOptionKey, adminUrl)
        .option("pulsar.admin.INVALID", "INVALID")
        .option(FailOnDataLossOptionKey, "true")
        .option(MaxBytesPerTrigger, approxSizeOfInt * 3)
        .load()
        .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
    }
    assert(e.getMessage.contains("invalid not supported by pulsar"))
  }

  test("Admission Control with one topic-partition") {
    val topic = newTopic()

    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      admin.topics().createPartitionedTopic(topic, 1)
      require(getLatestOffsets(Set(topic)).size === 1)
    }

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(AdminUrlOptionKey, adminUrl)
      .option(FailOnDataLossOptionKey, "true")
      .option(MaxBytesPerTrigger, approxSizeOfInt * 3)

    val pulsar = reader
      .option(TopicSingle, topic)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt)

    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1000)),
      makeSureGetOffsetCalled,
      AddPulsarDataWithPartition(topic, Some(0), 1, 2, 3, 4),
      AssertOnQuery { query =>
        query.recentProgress.map(microBatch =>
          microBatch.numInputRows <= 4
        ).forall(_ == true)
      }
    )
  }

  test("Admission Control with multiple topic-partitions") {
    val topic = newTopic()

    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      admin.topics().createPartitionedTopic(topic, 2)
      require(getLatestOffsets(Set(topic)).size === 2)
    }

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(AdminUrlOptionKey, adminUrl)
      .option(FailOnDataLossOptionKey, "true")
      .option(MaxBytesPerTrigger, approxSizeOfInt * 4)

    val pulsar = reader
      .option(TopicSingle, topic)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt)

    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1000)),
      makeSureGetOffsetCalled,
      AddPulsarDataWithPartition(topic, Some(0), 1, 2, 3, 4),
      AddPulsarDataWithPartition(topic, Some(1), 5, 6, 7, 8),
      AssertOnQuery { query =>
        query.recentProgress.map(microBatch =>
          microBatch.numInputRows <= 3
        ).forall(_ == true)
      }
    )
  }

  test("Add topic-partition after starting stream") {
    val topic = newTopic()

    Utils.tryWithResource(PulsarAdmin.builder().serviceHttpUrl(adminUrl).build()) { admin =>
      admin.topics().createPartitionedTopic(topic, 1)
      require(getLatestOffsets(Set(topic)).size === 1)
    }

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(AdminUrlOptionKey, adminUrl)
      .option(FailOnDataLossOptionKey, "true")
      .option(MaxBytesPerTrigger, approxSizeOfInt * 4)

    val pulsar = reader
      .option(TopicSingle, topic)
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt)

    testStream(mapped)(
      StartStream(trigger = ProcessingTime(1000)),
      makeSureGetOffsetCalled,
      AddPulsarDataWithPartition(topic, Some(0), 1, 2, 3, 4),
      AssertOnQuery { query =>
        query.recentProgress.map(microBatch =>
          microBatch.numInputRows <= 4
        ).forall(_ == true)
      }
    )

    addPartitions(topic, 2)

    testStream(mapped)(
      AddPulsarDataWithPartition(topic, Some(1), 5, 6, 7, 8),
      AssertOnQuery { query =>
        query.recentProgress.map(microBatch =>
          microBatch.numInputRows <= 3
        ).forall(_ == true)
      }
    )
  }
}
