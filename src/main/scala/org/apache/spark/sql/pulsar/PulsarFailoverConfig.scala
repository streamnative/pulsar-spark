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
import java.time.Duration
import java.util.Locale
import java.util.concurrent.TimeUnit

import scala.jdk.CollectionConverters._

import org.apache.pulsar.client.api.{
  AuthenticationFactory,
  AutoClusterFailoverBuilder,
  ServiceUrlProvider
}
import org.apache.pulsar.client.impl.AutoClusterFailover

import org.apache.spark.sql.pulsar.PulsarOptions._

private[pulsar] case class SecondaryClusterConfig(
    serviceUrl: String,
    auth: Option[(String, String)],
    tlsTrustCertsFilePath: Option[String],
    tlsTrustStorePath: Option[String],
    tlsTrustStorePassword: Option[String])

private[pulsar] case class PulsarFailoverConfig(
    primaryServiceUrl: String,
    secondaries: Seq[SecondaryClusterConfig],
    failoverDelay: Duration,
    switchBackDelay: Duration,
    checkInterval: Duration,
    policy: AutoClusterFailoverBuilder.FailoverPolicy)

private[pulsar] object PulsarFailoverConfig {

  private val DefaultFailoverDelayMs = 30000L
  private val DefaultSwitchBackDelayMs = 60000L
  private val DefaultCheckIntervalMs = 30000L

  private val SecondaryServiceUrl = "serviceurl"
  private val SecondaryAuthPluginClassName = "authpluginclassname"
  private val SecondaryAuthParams = "authparams"
  private val SecondaryTlsTrustCertsFilePath = "tlstrustcertsfilepath"
  private val SecondaryTlsTrustStorePath = "tlstruststorepath"
  private val SecondaryTlsTrustStorePassword = "tlstruststorepassword"

  private val TopLevelKeys = Set(
    PulsarFailoverPrimaryServiceUrlOptionKey,
    PulsarFailoverDelayMsOptionKey,
    PulsarFailoverSwitchBackDelayMsOptionKey,
    PulsarFailoverCheckIntervalMsOptionKey,
    PulsarFailoverPolicyOptionKey)

  private val SecondaryKeys = Set(
    SecondaryServiceUrl,
    SecondaryAuthPluginClassName,
    SecondaryAuthParams,
    SecondaryTlsTrustCertsFilePath,
    SecondaryTlsTrustStorePath,
    SecondaryTlsTrustStorePassword)

  def primaryServiceUrl(params: Map[String, String]): Option[String] = {
    normalize(params).get(PulsarFailoverPrimaryServiceUrlOptionKey).map(_.trim).filter(_.nonEmpty)
  }

  /** Returns Some(...) if any pulsar.failover.* key is present, else None. */
  def fromParams(params: ju.Map[String, Object]): Option[PulsarFailoverConfig] = {
    fromParams(params.asScala.toMap.collect { case (k, v) if v != null => k -> v.toString })
  }

  /** Returns Some(...) if any pulsar.failover.* key is present, else None. */
  def fromParams(params: Map[String, String]): Option[PulsarFailoverConfig] = {
    val normalized = normalize(params)
    val failoverParams = normalized.filter { case (k, _) =>
      k.startsWith(PulsarFailoverOptionKeyPrefix)
    }
    if (failoverParams.isEmpty) {
      return None
    }

    validateKnownKeys(failoverParams.keySet)

    if (!failoverParams.contains(PulsarFailoverPrimaryServiceUrlOptionKey)) {
      return None
    }

    val primary = requiredNonEmpty(
      failoverParams,
      PulsarFailoverPrimaryServiceUrlOptionKey,
      s"$PulsarFailoverPrimaryServiceUrlDisplayKey must be specified when Pulsar failover " +
        "is enabled")

    val secondaries = parseSecondaries(failoverParams)
    if (secondaries.isEmpty) {
      throw new IllegalArgumentException(
        s"${PulsarFailoverSecondaryDisplayPrefix}0.serviceUrl must be specified when Pulsar " +
          s"failover is enabled; use $ServiceUrlOptionKey directly if no secondary cluster " +
          "is needed")
    }

    Some(
      PulsarFailoverConfig(
        primary,
        secondaries,
        parsePositiveDuration(
          failoverParams,
          PulsarFailoverDelayMsOptionKey,
          DefaultFailoverDelayMs),
        parsePositiveDuration(
          failoverParams,
          PulsarFailoverSwitchBackDelayMsOptionKey,
          DefaultSwitchBackDelayMs),
        parsePositiveDuration(
          failoverParams,
          PulsarFailoverCheckIntervalMsOptionKey,
          DefaultCheckIntervalMs),
        parsePolicy(failoverParams)))
  }

  /** Build a ServiceUrlProvider from this config. */
  def toServiceUrlProvider(cfg: PulsarFailoverConfig): ServiceUrlProvider = {
    val secondaryServiceUrls = cfg.secondaries.map(_.serviceUrl).asJava
    val secondaryAuth =
      completeSecondaryMap[org.apache.pulsar.client.api.Authentication](cfg.secondaries) {
        secondary =>
          secondary.auth.map { case (pluginClassName, authParams) =>
            AuthenticationFactory.create(pluginClassName, authParams)
          }
      }
    val secondaryTlsTrustCertsFilePath =
      completeSecondaryMap[String](cfg.secondaries)(_.tlsTrustCertsFilePath)
    val secondaryTlsTrustStorePath =
      completeSecondaryMap[String](cfg.secondaries)(_.tlsTrustStorePath)
    val secondaryTlsTrustStorePassword =
      completeSecondaryMap[String](cfg.secondaries)(_.tlsTrustStorePassword)

    val builder = AutoClusterFailover
      .builder()
      .primary(cfg.primaryServiceUrl)
      .secondary(secondaryServiceUrls)
      .failoverPolicy(cfg.policy)
      .failoverDelay(cfg.failoverDelay.toMillis, TimeUnit.MILLISECONDS)
      .switchBackDelay(cfg.switchBackDelay.toMillis, TimeUnit.MILLISECONDS)
      .checkInterval(cfg.checkInterval.toMillis, TimeUnit.MILLISECONDS)
    secondaryAuth.foreach(auth => builder.secondaryAuthentication(auth.asJava))
    secondaryTlsTrustCertsFilePath.foreach(tls =>
      builder.secondaryTlsTrustCertsFilePath(tls.asJava))
    secondaryTlsTrustStorePath.foreach(tls => builder.secondaryTlsTrustStorePath(tls.asJava))
    secondaryTlsTrustStorePassword.foreach(tls =>
      builder.secondaryTlsTrustStorePassword(tls.asJava))
    builder.build()
  }

  private def completeSecondaryMap[T >: Null](secondaries: Seq[SecondaryClusterConfig])(
      value: SecondaryClusterConfig => Option[T]): Option[Map[String, T]] = {
    val values = secondaries.map(secondary => secondary.serviceUrl -> value(secondary))
    if (values.exists(_._2.isDefined)) {
      Some(values.map { case (serviceUrl, maybeValue) => serviceUrl -> maybeValue.orNull }.toMap)
    } else {
      None
    }
  }

  private def normalize(params: Map[String, String]): Map[String, String] = {
    params.map { case (k, v) => normalizeKey(k) -> v }
  }

  private def normalizeKey(key: String): String = {
    val lower = key.toLowerCase(Locale.ROOT)
    if (lower.startsWith(PulsarFailoverOptionKeyPrefix)) {
      PulsarFailoverOptionKeyPrefix +
        lower.substring(PulsarFailoverOptionKeyPrefix.length).replace(".", "")
    } else {
      lower.replace(".", "")
    }
  }

  private def validateKnownKeys(keys: Set[String]): Unit = {
    keys.foreach {
      case key if TopLevelKeys.contains(key) =>
      case key if parseSecondaryKey(key).exists { case (_, name) =>
            SecondaryKeys.contains(name)
          } =>
      case key =>
        throw new IllegalArgumentException(
          s"Unsupported Pulsar failover option: ${displayKey(key)}")
    }
  }

  private def parseSecondaryKey(key: String): Option[(Int, String)] = {
    val normalizedPrefix = normalizeKey(PulsarFailoverSecondaryPrefix)
    if (!key.startsWith(normalizedPrefix)) {
      return None
    }
    val rest = key.substring(normalizedPrefix.length)
    val index = rest.takeWhile(_.isDigit)
    val name = rest.drop(index.length)
    if (index.nonEmpty && name.nonEmpty) {
      Some(index.toInt -> name.stripPrefix("."))
    } else {
      None
    }
  }

  private def parseSecondaries(params: Map[String, String]): Seq[SecondaryClusterConfig] = {
    val grouped = params.toSeq
      .flatMap { case (key, value) =>
        parseSecondaryKey(key).map { case (index, name) => index -> (name -> value) }
      }
      .groupBy(_._1)
      .map { case (index, entries) =>
        index -> entries.map(_._2).toMap
      }

    if (grouped.isEmpty) {
      return Seq.empty
    }

    val indexes = grouped.keys.toSeq.sorted
    val expected = indexes.head to indexes.last
    if (indexes.head != 0 || indexes != expected) {
      val missing = expected.filterNot(grouped.contains)
      throw new IllegalArgumentException(
        s"Pulsar failover secondary indexes must start at 0 and be continuous; " +
          s"configured indexes: ${indexes.mkString(",")}, missing indexes: " +
          (if (missing.nonEmpty) missing.mkString(",") else "before 0"))
    }

    indexes.map { index =>
      val secondaryParams = grouped(index)
      val serviceUrl = requiredNonEmpty(
        secondaryParams,
        SecondaryServiceUrl,
        s"${PulsarFailoverSecondaryDisplayPrefix}$index.serviceUrl must be specified")
      val auth = parseAuth(secondaryParams, index)
      SecondaryClusterConfig(
        serviceUrl,
        auth,
        nonEmpty(secondaryParams, SecondaryTlsTrustCertsFilePath),
        nonEmpty(secondaryParams, SecondaryTlsTrustStorePath),
        nonEmpty(secondaryParams, SecondaryTlsTrustStorePassword))
    }
  }

  private def parseAuth(params: Map[String, String], index: Int): Option[(String, String)] = {
    val pluginClassName = nonEmpty(params, SecondaryAuthPluginClassName)
    val authParams = nonEmpty(params, SecondaryAuthParams)
    (pluginClassName, authParams) match {
      case (Some(pluginClassName), Some(authParams)) => Some(pluginClassName -> authParams)
      case (None, None) => None
      case _ =>
        throw new IllegalArgumentException(
          s"${PulsarFailoverSecondaryDisplayPrefix}$index.authPluginClassName and " +
            s"${PulsarFailoverSecondaryDisplayPrefix}$index.authParams must be specified " +
            "together")
    }
  }

  private def parsePositiveDuration(
      params: Map[String, String],
      key: String,
      defaultMs: Long): Duration = {
    val millis = params.get(key).map(_.trim).filter(_.nonEmpty) match {
      case Some(value) =>
        try {
          value.toLong
        } catch {
          case _: NumberFormatException =>
            throw new IllegalArgumentException(
              s"${displayKey(key)} must be a positive milliseconds value: $value")
        }
      case None => defaultMs
    }
    if (millis <= 0) {
      throw new IllegalArgumentException(s"${displayKey(key)} must be positive, but was $millis")
    }
    Duration.ofMillis(millis)
  }

  private def parsePolicy(
      params: Map[String, String]): AutoClusterFailoverBuilder.FailoverPolicy = {
    params.get(PulsarFailoverPolicyOptionKey).map(_.trim).filter(_.nonEmpty) match {
      case Some(policy) =>
        try {
          AutoClusterFailoverBuilder.FailoverPolicy.valueOf(policy.toUpperCase(Locale.ROOT))
        } catch {
          case _: IllegalArgumentException =>
            throw new IllegalArgumentException(s"Unsupported Pulsar failover policy: $policy")
        }
      case None => AutoClusterFailoverBuilder.FailoverPolicy.ORDER
    }
  }

  private def requiredNonEmpty(
      params: Map[String, String],
      key: String,
      message: String): String = {
    nonEmpty(params, key).getOrElse(throw new IllegalArgumentException(message))
  }

  private def nonEmpty(params: Map[String, String], key: String): Option[String] = {
    params.get(key).map(_.trim).filter(_.nonEmpty)
  }

  private def displayKey(key: String): String = {
    key match {
      case PulsarFailoverPrimaryServiceUrlOptionKey => PulsarFailoverPrimaryServiceUrlDisplayKey
      case PulsarFailoverDelayMsOptionKey => PulsarFailoverDelayMsDisplayKey
      case PulsarFailoverSwitchBackDelayMsOptionKey => PulsarFailoverSwitchBackDelayMsDisplayKey
      case PulsarFailoverCheckIntervalMsOptionKey => PulsarFailoverCheckIntervalMsDisplayKey
      case PulsarFailoverPolicyOptionKey => PulsarFailoverPolicyOptionKey
      case secondaryKey =>
        parseSecondaryKey(secondaryKey) match {
          case Some((index, SecondaryServiceUrl)) =>
            s"${PulsarFailoverSecondaryDisplayPrefix}$index.serviceUrl"
          case Some((index, SecondaryAuthPluginClassName)) =>
            s"${PulsarFailoverSecondaryDisplayPrefix}$index.authPluginClassName"
          case Some((index, SecondaryAuthParams)) =>
            s"${PulsarFailoverSecondaryDisplayPrefix}$index.authParams"
          case Some((index, SecondaryTlsTrustCertsFilePath)) =>
            s"${PulsarFailoverSecondaryDisplayPrefix}$index.tlsTrustCertsFilePath"
          case Some((index, SecondaryTlsTrustStorePath)) =>
            s"${PulsarFailoverSecondaryDisplayPrefix}$index.tlsTrustStorePath"
          case Some((index, SecondaryTlsTrustStorePassword)) =>
            s"${PulsarFailoverSecondaryDisplayPrefix}$index.tlsTrustStorePassword"
          case _ => secondaryKey
        }
    }
  }
}
