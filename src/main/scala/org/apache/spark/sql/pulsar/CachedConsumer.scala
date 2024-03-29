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

import java.util.concurrent.TimeUnit

import scala.util.{Failure, Success, Try}

import com.google.common.cache._
import org.apache.pulsar.client.api.{Consumer, PulsarClient, SubscriptionInitialPosition}
import org.apache.pulsar.client.api.schema.GenericRecord
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

private[pulsar] object CachedConsumer extends Logging {

  private var client: PulsarClient = null

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long =
    Option(SparkEnv.get)
      .map(
        _.conf
          .getTimeAsMs(
            "spark.pulsar.client.cache.timeout",
            s"${defaultCacheExpireTimeout}ms")) match {
      case Some(timeout) => timeout
      case None => defaultCacheExpireTimeout
    }

  private val cacheLoader = new CacheLoader[(String, String), Consumer[GenericRecord]]() {
    override def load(k: (String, String)): Consumer[GenericRecord] = {
      val (topic, subscription) = k
      Try(
        client
          .newConsumer(new AutoConsumeSchema())
          .topic(topic)
          .subscriptionName(subscription)
          .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
          .subscribe()) match {
        case Success(consumer) => consumer
        case Failure(exception) =>
          logError(
            s"Failed to create consumer to topic ${topic} with subscription ${subscription}")
          throw exception
      }
    }
  }

  private val removalListener = new RemovalListener[(String, String), Consumer[GenericRecord]]() {
    override def onRemoval(
        notification: RemovalNotification[(String, String), Consumer[GenericRecord]]): Unit = {
      Try(notification.getValue.close()) match {
        case Success(_) => logInfo(s"Closed consumer for ${notification.getKey}")
        case Failure(exception) =>
          logWarning(s"Failed to close consumer for ${notification.getKey}", exception)
      }
    }
  }

  private lazy val guavaCache: LoadingCache[(String, String), Consumer[GenericRecord]] =
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[(String, String), Consumer[GenericRecord]](cacheLoader)

  private[pulsar] def getOrCreate(
      topic: String,
      subscription: String,
      client: PulsarClient): Consumer[GenericRecord] = {
    this.client = client
    Try(guavaCache.get((topic, subscription))) match {
      case Success(consumer) => consumer
      case Failure(exception) =>
        logError(s"Failed to create consumer to topic ${topic} with subscription ${subscription}")
        throw exception
    }
  }

  private[pulsar] def close(topic: String, subscription: String): Unit = {
    guavaCache.invalidate((topic, subscription))
  }

  private[pulsar] def clear(): Unit = {
    logInfo("Cleaning up Consumer Cache.")
    guavaCache.invalidateAll()
  }

}
