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

import java.util.concurrent.{ConcurrentMap, ExecutionException, TimeUnit}
import java.{util => ju}

import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

private[pulsar] object CachedPulsarProducer extends Logging {

  private type K = (Seq[(String, Object)], Seq[(String, Object)], String)
  private type JUK = (ju.Map[String, Object], ju.Map[String, Object], String)

  private type V = org.apache.pulsar.client.api.Producer[Array[Byte]]

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long =
    Option(SparkEnv.get).map(_.conf.getTimeAsMs(
      "spark.pulsar.producer.cache.timeout",
      s"${defaultCacheExpireTimeout}ms")).getOrElse(defaultCacheExpireTimeout)

  private val cacheLoader = new CacheLoader[K, V] {
    override def load(config: K): V = {
      createPulsarClient((config._1.toMap.asJava, config._2.toMap.asJava, config._3))
    }
  }

  private val removalListener = new RemovalListener[K, V]() {
    override def onRemoval(
        notification: RemovalNotification[K, V]): Unit = {
      val paramsSeq: K = notification.getKey
      val client: V = notification.getValue
      logDebug(
        s"Evicting pulsar producer $client params: $paramsSeq, due to ${notification.getCause}")
      close(paramsSeq, client)
    }
  }

  private lazy val guavaCache: LoadingCache[K, V] =
    CacheBuilder.newBuilder().expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[K, V](cacheLoader)

  private def createPulsarClient(config: JUK): V = {
    val (clientConf, producerConf, topic) = config
    logInfo(s"clientConf = $clientConf, producerConf = $producerConf, topic = $topic")
    try {
      val producer = CachedPulsarClient.getOrCreate(clientConf)
        .newProducer()
        .topic(topic)
        .loadConf(producerConf)
        .batchingMaxPublishDelay(100, TimeUnit.MILLISECONDS)
        // maximizing the throughput
        .batchingMaxMessages(5 * 1024 * 1024)
        .create()

      logDebug(s"Created a new instance of Producer for $config")
      producer
    } catch {
      case e: Throwable =>
        logError(s"Failed to create Producer using $config", e)
        throw e
    }
  }

  /**
    * Get a cached PulsarProducer for a given configuration. If matching PulsarProducer doesn't
    * exist, a new PulsarProducer will be created. PulsarProducer is thread safe, it is best to keep
    * one instance per specified pulsarParams.
    */
  private[pulsar] def getOrCreate(config: JUK): V = {
    try {
      guavaCache.get(juk2k(config))
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
        if e.getCause != null =>
        throw e.getCause
    }
  }

  def juk2k(config: JUK): K = {
    def paramsToSeq(params: ju.Map[String, Object]): Seq[(String, Object)] = {
      val paramsSeq: Seq[(String, Object)] = params.asScala.toSeq.sortBy(x => x._1)
      paramsSeq
    }
    (paramsToSeq(config._1), paramsToSeq(config._2), config._3)
  }

  /** For explicitly closing pulsar producer */
  private[pulsar] def close(config: JUK): Unit = {
    guavaCache.invalidate(juk2k(config))
  }

  /** Auto close on cache evict */
  private def close(config: K, producer: V): Unit = {
    try {
      logInfo(s"Closing the Pulsar producer with params: $config.")
      producer.close()
    } catch {
      case NonFatal(e) => logWarning("Error while closing pulsar producer.", e)
    }
  }

  private[pulsar] def clear(): Unit = {
    logInfo("Cleaning up guava cache.")
    guavaCache.invalidateAll()
  }

  // Intended for testing purpose only.
  private def getAsMap: ConcurrentMap[K, V] = guavaCache.asMap()
}
