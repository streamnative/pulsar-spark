package org.apache.spark.sql.pulsar

import java.util.concurrent.{ExecutionException, TimeUnit}
import scala.util.control.NonFatal
import com.google.common.cache._
import com.google.common.util.concurrent.{ExecutionError, UncheckedExecutionException}
import org.apache.pulsar.client.api.{Consumer, PulsarClient}
import org.apache.pulsar.client.impl.schema.AutoConsumeSchema
import org.apache.pulsar.client.api.schema.GenericRecord
import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging

private[pulsar] object CachedConsumer extends Logging {

  private var client: Option[PulsarClient] = None

  private val defaultCacheExpireTimeout = TimeUnit.MINUTES.toMillis(10)

  private lazy val cacheExpireTimeout: Long =
    Option(SparkEnv.get)
      .map(_.conf
        .getTimeAsMs("spark.pulsar.client.cache.timeout", s"${defaultCacheExpireTimeout}ms"))
      .getOrElse(defaultCacheExpireTimeout)

  private val cacheLoader = new CacheLoader[(String, String), Consumer[GenericRecord]]() {
    override def load(k: (String, String)): Consumer[GenericRecord] = {
      val (topic, subscription) = (k._1, k._2)
      try {
        val consumer = client.get
          .newConsumer(new AutoConsumeSchema())
          .topic(topic)
          .subscriptionName(subscription)
          .subscribe()

        consumer
      } catch {
        case e: Throwable =>
          logError(
            s"Failed to create consumer to topic ${topic} with subscription ${subscription}")
          throw e
      }
    }
  }

  private val removalListener = new RemovalListener[(String, String), Consumer[GenericRecord]]() {
    override def onRemoval(
        notification: RemovalNotification[(String, String), Consumer[GenericRecord]]): Unit = {
      val (topic, subscription) = (notification.getKey._1, notification.getKey._2)
      val consumer = notification.getValue

      try {
        consumer.close()
      } catch {
        case NonFatal(e) => logWarning("Error while closing consumer.", e)
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
    try {
      this.client = Some(client)
      guavaCache.get((topic, subscription))
    } catch {
      case e @ (_: ExecutionException | _: UncheckedExecutionException | _: ExecutionError)
          if e.getCause != null =>
        throw e.getCause
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
