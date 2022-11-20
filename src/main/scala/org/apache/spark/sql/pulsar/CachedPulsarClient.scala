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

import java.{util => ju}
import java.util.concurrent.{ExecutionException, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.control.NonFatal

import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.pulsar.client.api.PulsarClient

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.pulsar.PulsarOptions._

private[pulsar] object CachedPulsarClient extends Logging {

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long =
    Option(SparkEnv.get)
      .map(_.conf
        .getTimeAsMs("spark.pulsar.client.cache.timeout", s"${defaultCacheExpireTimeout}ms"))
      .getOrElse(defaultCacheExpireTimeout)

  private val cacheLoader = new CacheLoader[ju.Map[String, Object], PulsarClient]() {
    override def load(config: ju.Map[String, Object]): PulsarClient = {
      val pulsarServiceUrl = config.get(PulsarOptions.ServiceUrlOptionKey).toString
      val clientConf =
        PulsarConfigUpdater("pulsarClientCache", config.asScala.toMap, PulsarOptions.FilteredKeys)
          .rebuild()
      logInfo(s"Client Conf = ${clientConf}")

      val builder = PulsarClient.builder()
      try {
        builder
          .loadConf(clientConf)
          .serviceUrl(pulsarServiceUrl)

        // Set authentication parameters.
        if (clientConf.containsKey(AuthPluginClassName)) {
          builder.authentication(
            clientConf.get(AuthPluginClassName).toString,
            clientConf.get(AuthParams).toString)
        }

        val pulsarClient: PulsarClient = builder.build()
        logDebug(
          s"Created a new instance of PulsarClient for serviceUrl = $pulsarServiceUrl,"
            + s" clientConf = $clientConf.")

        pulsarClient
      } catch {
        case e: Throwable =>
          logError(
            s"Failed to create PulsarClient to serviceUrl ${pulsarServiceUrl}"
              + s" using client conf ${clientConf}",
            e)
          throw e
      }
    }
  }

  private val removalListener = new RemovalListener[ju.Map[String, Object], PulsarClient]() {
    override def onRemoval(
        notification: RemovalNotification[ju.Map[String, Object], PulsarClient]): Unit = {
      val params: ju.Map[String, Object] = notification.getKey
      val client: PulsarClient = notification.getValue
      logDebug(
        s"Evicting pulsar producer $client params: $params, due to ${notification.getCause}")

      // Close client on cache evict.
      try {
        logInfo(s"Closing the Pulsar Client with params: $params.")
        client.close()
      } catch {
        case NonFatal(e) => logWarning("Error while closing pulsar producer.", e)
      }
    }
  }

  private lazy val guavaCache: LoadingCache[ju.Map[String, Object], PulsarClient] =
    CacheBuilder
      .newBuilder()
      .expireAfterAccess(cacheExpireTimeout, TimeUnit.MILLISECONDS)
      .removalListener(removalListener)
      .build[ju.Map[String, Object], PulsarClient](cacheLoader)

  /**
   * Get a cached PulsarProducer for a given configuration. If matching PulsarProducer doesn't
   * exist, a new PulsarProducer will be created. PulsarProducer is thread safe, it is best to
   * keep one instance per specified pulsarParams.
   */
  private[pulsar] def getOrCreate(params: ju.Map[String, Object]): PulsarClient = {
    try {
      guavaCache.get(params)
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
          if e.getCause != null =>
        throw e.getCause
    }
  }

  /** For explicitly closing pulsar producer */
  private[pulsar] def close(params: ju.Map[String, Object]): Unit = {
    guavaCache.invalidate(params)
  }

  private[pulsar] def clear(): Unit = {
    logInfo("Cleaning up guava cache.")
    guavaCache.invalidateAll()
  }
}
