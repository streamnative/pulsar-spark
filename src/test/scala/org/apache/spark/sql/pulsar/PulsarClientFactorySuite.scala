package org.apache.spark.sql.pulsar

import org.apache.pulsar.client.impl.PulsarClientImpl
import org.apache.spark.sql.pulsar.PulsarOptions.{ServiceUrlOptionKey, TopicPattern}
import org.apache.spark.sql.pulsar.TestPulsarClientFactory.counter

import java.{util => ju}

class TestPulsarClientFactory extends PulsarClientFactory {
  def getOrCreate(params: ju.Map[String, Object]): PulsarClientImpl = {
    counter += 1
    new DefaultPulsarClientFactory().getOrCreate(params)
  }
}

object TestPulsarClientFactory {
  var counter = 0
}

class PulsarClientFactorySuite extends PulsarSourceTest {
  test("Set Pulsar client factory class") {
    sparkContext.conf.set(PulsarClientFactory.PulsarClientFactoryClassOption,
      "org.apache.spark.sql.pulsar.TestPulsarClientFactory")
    val topic = newTopic()
    sendMessages(topic, (101 to 105).map { _.toString }.toArray)

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicPattern, s"$topic.*")

    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")

    testStream(pulsar)(
      makeSureGetOffsetCalled,
      StopStream
    )
    // Assert that we are using the test factory.
    assert(TestPulsarClientFactory.counter > 0)
  }

  test("Unset Pulsar client factory class") {
    sparkContext.conf.remove(PulsarClientFactory.PulsarClientFactoryClassOption)
    val oldCount = TestPulsarClientFactory.counter
    val topic = newTopic()
    sendMessages(topic, (101 to 105).map { _.toString }.toArray)

    val reader = spark.readStream
      .format("pulsar")
      .option(ServiceUrlOptionKey, serviceUrl)
      .option(TopicPattern, s"$topic.*")

    val pulsar = reader
      .load()
      .selectExpr("CAST(__key AS STRING)", "CAST(value AS STRING)")

    testStream(pulsar)(
      makeSureGetOffsetCalled,
      StopStream
    )

    val newCount = TestPulsarClientFactory.counter
    // The count doesn't change because we are using the default factory.
    assert(oldCount == newCount)
  }
}
