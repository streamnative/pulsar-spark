package org.apache.spark.sql.pulsar

import org.apache.spark.sql.pulsar.PulsarOptions.{FailOnDataLossOptionKey, ServiceUrlOptionKey, StartingOffsetsOptionKey, StartingTime, TopicMulti, TopicPattern, TopicSingle}
import org.apache.spark.sql.streaming.Trigger

class PulsarMicroBatchSourceTriggerAvailableNowSuite extends PulsarSourceTest {
  import testImplicits._
  override val defaultTrigger: Trigger =  Trigger.AvailableNow()
  val failOnDataLoss = true

  test(s"assign from latest offsets: available now") {
    val topic = newTopic()
    testFromLatestOffsets(
      topic,
      addPartitions = false,
      failOnDataLoss = failOnDataLoss,
      TopicSingle -> topic)
  }

  test(s"assign from earliest offsets: available now") {
    val topic = newTopic()
    testFromEarliestOffsets(
      topic,
      addPartitions = false,
      failOnDataLoss = failOnDataLoss,
      TopicSingle -> topic)
  }

  test(s"assign from time: available now") {
    val topic = newTopic()
    testFromTime(
      topic,
      addPartitions = false,
      failOnDataLoss = failOnDataLoss,
      TopicSingle -> topic)
  }

  test(s"assign from specific offsets: available now") {
    val topic = newTopic()
    testFromSpecificOffsets(
      topic,
      failOnDataLoss = failOnDataLoss,
      TopicSingle -> topic,
      FailOnDataLossOptionKey -> failOnDataLoss.toString)
  }

  test(s"subscribing topic by name from latest offsets: available now") {
    val topic = newTopic()
    testFromLatestOffsets(
      topic,
      addPartitions = true,
      failOnDataLoss = failOnDataLoss,
      TopicMulti -> topic)
  }

  test(s"subscribing topic by name from earliest offsets: available now") {
    val topic = newTopic()
    testFromEarliestOffsets(
      topic,
      addPartitions = true,
      failOnDataLoss = failOnDataLoss,
      TopicMulti -> topic)
  }

  test(s"subscribing topic by name from specific offsets: available now") {
    val topic = newTopic()
    testFromSpecificOffsets(topic, failOnDataLoss = failOnDataLoss, TopicMulti -> topic)
  }

  test(s"subscribing topic by pattern from latest offsets: available now") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromLatestOffsets(
      topic,
      addPartitions = true,
      failOnDataLoss = failOnDataLoss,
      TopicPattern -> s"$topicPrefix-.*")
  }

  test(s"subscribing topic by pattern from earliest offsets: available now") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromEarliestOffsets(
      topic,
      addPartitions = true,
      failOnDataLoss = failOnDataLoss,
      TopicPattern -> s"$topicPrefix-.*")
  }

  test(s"subscribing topic by pattern from specific offsets: available now") {
    val topicPrefix = newTopic()
    val topic = topicPrefix + "-suffix"
    testFromSpecificOffsets(
      topic,
      failOnDataLoss = failOnDataLoss,
      TopicPattern -> s"$topicPrefix-.*")
  }

  private def testFromLatestOffsets(
    topic: String,
    addPartitions: Boolean,
    failOnDataLoss: Boolean,
    options: (String, String)*): Unit = {

    sendMessages(topic, Array("-1", "0", "1"))
    require(getLatestOffsets(Set(topic)).size === 1)

    val reader = spark.readStream
      .format("pulsar")
      .option(StartingOffsetsOptionKey, "latest")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, failOnDataLoss.toString)

    options.foreach { case (k, v) => reader.option(k, v) }
    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      AddPulsarData(Set(topic), 2, 3),
      StopStream,
      StartStream(),
      CheckAnswer(3, 4),
      AddPulsarData(Set(topic), 4, 5, 6),
      StopStream,
      StartStream(),
      CheckAnswer(3, 4, 5, 6, 7),
      AddPulsarData(Set(topic), 7, 8),
      StopStream,
      StartStream(),
      CheckAnswer(3, 4, 5, 6, 7, 8, 9),
    )
  }

  private def testFromEarliestOffsets(
    topic: String,
    addPartitions: Boolean,
    failOnDataLoss: Boolean,
    options: (String, String)*): Unit = {

    sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(getLatestOffsets(Set(topic)).size === 1)

    val reader = spark.readStream
    reader
      .format("pulsar")
      .option(StartingOffsetsOptionKey, "earliest")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, failOnDataLoss.toString)
    options.foreach { case (k, v) => reader.option(k, v) }
    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt + 1)

    testStream(mapped)(
      AddPulsarData(Set(topic), 4, 5, 6), // Add data when stream is stopped
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7),
      AddPulsarData(Set(topic), 7, 8),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9),
      AddPulsarData(Set(topic), 9, 10, 11, 12, 13, 14, 15, 16),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17)
    )
  }

  private def testFromTime(
    topic: String,
    addPartitions: Boolean,
    failOnDataLoss: Boolean,
    options: (String, String)*): Unit = {

    val time0 = System.currentTimeMillis() - 10000

    sendMessages(topic, (1 to 3).map { _.toString }.toArray)
    require(getLatestOffsets(Set(topic)).size === 1)

    def dfAfter(ts: Long) = {
      val reader = spark.readStream
      reader
        .format("pulsar")
        .option(StartingTime, time0)
        .option(ServiceUrlOptionKey, serviceUrl)
        .option(FailOnDataLossOptionKey, failOnDataLoss.toString)
      options.foreach { case (k, v) => reader.option(k, v) }
      val pulsar = reader
        .load()
        .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
        .as[(String, String)]
      val mapped = pulsar.map(kv => kv._2.toInt + 1)
      mapped
    }

    testStream(dfAfter(time0))(
      AddPulsarData(Set(topic), 7, 8, 9),
      StopStream,
      StartStream(),
      CheckAnswer(2, 3, 4, 8, 9, 10)
    )
  }

  private def testFromSpecificOffsets(
    topic: String,
    failOnDataLoss: Boolean,
    options: (String, String)*): Unit = {

    val mids = sendMessages(
      topic,
      Array(
        //  0,   1,   2,  3, 4, 5,  6, 7,  8
        -20, -21, -22, 1, 2, 3, 10, 11, 12).map(_.toString),
      None).map(_._2)

    val s1 = JsonUtils.topicOffsets(Map(topic -> mids(3)))

    val reader = spark.readStream
      .format("pulsar")
      .option(StartingOffsetsOptionKey, s1)
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(FailOnDataLossOptionKey, failOnDataLoss.toString)
    options.foreach { case (k, v) => reader.option(k, v) }
    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")
      .as[(String, String)]
    val mapped = pulsar.map(kv => kv._2.toInt)

    testStream(mapped)(
      AddPulsarData(Set(topic), 7),
      StopStream,
      StartStream(),
      CheckAnswer(1, 2, 3, 10, 11, 12, 7),
      AddPulsarData(Set(topic), 30, 31, 32, 33, 34),
      StopStream,
      StartStream(),
      CheckAnswer(1, 2, 3, 10, 11, 12, 7, 30, 31, 32, 33, 34)
    )
  }
}
