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
import org.apache.pulsar.client.api.SubscriptionType
import org.apache.pulsar.client.api.schema.GenericRecord
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

private[pulsar] object CachedConsumer extends Logging {

  private type CacheKey = (String, String, SubscriptionType)

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

  private val cacheLoader = new CacheLoader[CacheKey, Consumer[GenericRecord]]() {
    override def load(k: CacheKey): Consumer[GenericRecord] = {
      val (topic, subscription, subscriptionType) = k
      val consumerBuilder = client
        .newConsumer(new AutoConsumeSchema())
        .topic(topic)
        .subscriptionName(subscription)
        .subscriptionType(subscriptionType)
        .subscriptionInitialPosition(SubscriptionInitialPosition.Earliest)
      // This consumer is only used for cursor management and never receives messages.
      // On a shared subscription a non-zero receiver queue would prefetch messages and
      // withhold them from the other consumers attached to the same subscription.
      if (subscriptionType == SubscriptionType.Shared
        || subscriptionType == SubscriptionType.Key_Shared) {
        consumerBuilder.receiverQueueSize(0)
      }
      Try(consumerBuilder.subscribe()) match {
        case Success(consumer) => consumer
        case Failure(exception) =>
          logError(
            s"Failed to create consumer to topic ${topic} with subscription ${subscription}")
          throw exception
      }
    }
  }

  private val removalListener = new RemovalListener[CacheKey, Consumer[GenericRecord]]() {
    override def onRemoval(
        notification: RemovalNotification[CacheKey, Consumer[GenericRecord]]): Unit = {
      Try(notification.getValue.close()) match {
        case Success(_) => logInfo(s"Closed consumer for ${notification.getKey}")
        case Failure(exception) =>
          logWarning(s"Failed to close consumer for ${notification.getKey}", exception)
      }
    }
  }

  private lazy val guavaCache: LoadingCache[CacheKey, Consumer[GenericRecord]] =
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[CacheKey, Consumer[GenericRecord]](cacheLoader)

  private[pulsar] def getOrCreate(
      topic: String,
      subscription: String,
      client: PulsarClient,
      subscriptionType: SubscriptionType = SubscriptionType.Exclusive): Consumer[GenericRecord] = {
    this.client = client
    Try(guavaCache.get((topic, subscription, subscriptionType))) match {
      case Success(consumer) => consumer
      case Failure(exception) =>
        logError(s"Failed to create consumer to topic ${topic} with subscription ${subscription}")
        throw exception
    }
  }

  private[pulsar] def close(topic: String, subscription: String): Unit = {
    SubscriptionType.values().foreach { subscriptionType =>
      guavaCache.invalidate((topic, subscription, subscriptionType))
    }
  }

  private[pulsar] def clear(): Unit = {
    logInfo("Cleaning up Consumer Cache.")
    guavaCache.invalidateAll()
  }

}
