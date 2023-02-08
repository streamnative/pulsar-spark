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

import java.util

import scala.collection.JavaConverters._

import com.codahale.metrics.{Gauge, Metric, MetricRegistry, MetricSet}
import com.google.common.collect.ImmutableMap
import org.apache.pulsar.client.admin.PulsarAdmin
import org.apache.pulsar.common.policies.data.SubscriptionStats

import org.apache.spark.metrics.source.Source

/**
 * The metrics for exposing Pulsar's metrics to the Spark metric system.
 */
private class PulsarMetrics(admin: PulsarAdmin, topics: Seq[String], subscriptionPrefix: String)
    extends Source {

  override val sourceName = "pulsar"
  override val metricRegistry = new MetricRegistry

  topics.foreach(topic => registerSubscription(topic))

  private def registerSubscription(topic: String): Unit = {
    metricRegistry.registerAll(
      subscriptionPrefix,
      new MetricSet {
        override def getMetrics: util.Map[String, Metric] = {
          subscriptionStats(topic, subscriptionPrefix) match {
            case None => new util.HashMap()
            case Some(stats) =>
              ImmutableMap
                .builder[String, Metric]()
                .put(
                  "msgRateOut",
                  new Gauge[Double] {
                    override def getValue: Double = stats.getMsgRateOut
                  })
                .put(
                  "msgThroughputOut",
                  new Gauge[Double] {
                    override def getValue: Double = stats.getMsgThroughputOut
                  })
                .put(
                  "bytesOutCounter",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getBytesOutCounter
                  })
                .put(
                  "msgOutCounter",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getMsgOutCounter
                  })
                .put(
                  "msgRateRedeliver",
                  new Gauge[Double] {
                    override def getValue: Double = stats.getMsgRateRedeliver
                  })
                .put(
                  "messageAckRate",
                  new Gauge[Double] {
                    override def getValue: Double = stats.getMessageAckRate
                  })
                .put(
                  "chunkedMessageRate",
                  new Gauge[Int] {
                    override def getValue: Int = stats.getChunkedMessageRate
                  })
                .put(
                  "msgBacklog",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getMsgBacklog
                  })
                .put(
                  "backlogSize",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getBacklogSize
                  })
                .put(
                  "earliestMsgPublishTimeInBacklog",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getEarliestMsgPublishTimeInBacklog
                  })
                .put(
                  "msgBacklogNoDelayed",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getMsgBacklogNoDelayed
                  })
                .put(
                  "msgDelayed",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getMsgDelayed
                  })
                .put(
                  "unackedMessages",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getUnackedMessages
                  })
                .put(
                  "msgRateExpired",
                  new Gauge[Double] {
                    override def getValue: Double = stats.getMsgRateExpired
                  })
                .put(
                  "totalMsgExpired",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getTotalMsgExpired
                  })
                .put(
                  "lastExpireTimestamp",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getLastExpireTimestamp
                  })
                .put(
                  "lastConsumedFlowTimestamp",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getLastConsumedFlowTimestamp
                  })
                .put(
                  "lastConsumedTimestamp",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getLastConsumedTimestamp
                  })
                .put(
                  "lastAckedTimestamp",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getLastAckedTimestamp
                  })
                .put(
                  "lastMarkDeleteAdvancedTimestamp",
                  new Gauge[Long] {
                    override def getValue: Long = stats.getLastMarkDeleteAdvancedTimestamp
                  })
                .build()
          }
        }
      })
  }

  private def subscriptionStats(
      topic: String,
      subscriptionPrefix: String): Option[SubscriptionStats] = {
    val subscriptions = admin.topics().getStats(topic).getSubscriptions.asScala
    subscriptions.keys.find(sub => sub.startsWith(subscriptionPrefix)) match {
      case Some(sub) => subscriptions.get(sub)
      case _ => None
    }
  }
}
