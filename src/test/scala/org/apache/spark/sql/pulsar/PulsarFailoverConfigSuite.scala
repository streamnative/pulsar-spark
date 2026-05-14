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

import java.time.Duration

import scala.jdk.CollectionConverters._

import org.apache.pulsar.client.api.AutoClusterFailoverBuilder.FailoverPolicy

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.pulsar.PulsarOptions._

class PulsarFailoverConfigSuite extends SparkFunSuite {

  private val primaryUrl = "pulsar://primary:6650"
  private val secondary0Url = "pulsar://secondary-0:6650"
  private val secondary1Url = "pulsar://secondary-1:6650"

  test("fromParams returns None when no failover option is configured") {
    assert(PulsarFailoverConfig.fromParams(Map(ServiceUrlOptionKey -> primaryUrl)).isEmpty)
  }

  test("missing secondary.0.serviceUrl fails with guidance to use service.url") {
    val error = intercept[IllegalArgumentException] {
      PulsarFailoverConfig.fromParams(Map(PulsarFailoverPrimaryServiceUrlOptionKey -> primaryUrl))
    }

    assert(error.getMessage.contains("secondary.0.serviceUrl"))
    assert(error.getMessage.contains(ServiceUrlOptionKey))
  }

  test("secondary indexes must start from 0 and be continuous") {
    val error = intercept[IllegalArgumentException] {
      PulsarFailoverConfig.fromParams(
        Map(
          PulsarFailoverPrimaryServiceUrlOptionKey -> primaryUrl,
          s"${PulsarFailoverSecondaryPrefix}0.serviceUrl" -> secondary0Url,
          s"${PulsarFailoverSecondaryPrefix}2.serviceUrl" -> secondary1Url))
    }

    assert(error.getMessage.contains("continuous"))
    assert(error.getMessage.contains("missing indexes: 1"))
  }

  test("non-positive duration fails") {
    val error = intercept[IllegalArgumentException] {
      PulsarFailoverConfig.fromParams(
        Map(
          PulsarFailoverPrimaryServiceUrlOptionKey -> primaryUrl,
          s"${PulsarFailoverSecondaryPrefix}0.serviceUrl" -> secondary0Url,
          PulsarFailoverDelayMsOptionKey -> "0"))
    }

    assert(error.getMessage.contains(PulsarFailoverDelayMsDisplayKey))
    assert(error.getMessage.contains("positive"))
  }

  test("documented option keys parse case-insensitively") {
    val config = PulsarFailoverConfig
      .fromParams(
        Map(
          PulsarFailoverPrimaryServiceUrlDisplayKey.toUpperCase(java.util.Locale.ROOT) ->
            primaryUrl,
          PulsarFailoverDelayMsDisplayKey -> "5000",
          PulsarFailoverSwitchBackDelayMsDisplayKey -> "10000",
          PulsarFailoverCheckIntervalMsDisplayKey -> "15000",
          s"${PulsarFailoverSecondaryPrefix}0.serviceUrl" -> secondary0Url))
      .get

    assert(config.primaryServiceUrl === primaryUrl)
    assert(config.failoverDelay === Duration.ofMillis(5000))
    assert(config.switchBackDelay === Duration.ofMillis(10000))
    assert(config.checkInterval === Duration.ofMillis(15000))
    assert(config.secondaries.map(_.serviceUrl) === Seq(secondary0Url))
  }

  test("full config parses durations, policy, auth and tls per secondary") {
    val config = PulsarFailoverConfig
      .fromParams(
        Map(
          PulsarFailoverPrimaryServiceUrlOptionKey -> primaryUrl,
          PulsarFailoverDelayMsOptionKey -> "5000",
          PulsarFailoverSwitchBackDelayMsOptionKey -> "10000",
          PulsarFailoverCheckIntervalMsOptionKey -> "15000",
          PulsarFailoverPolicyOptionKey -> "order",
          s"${PulsarFailoverSecondaryPrefix}0.serviceUrl" -> secondary0Url,
          s"${PulsarFailoverSecondaryPrefix}0.authPluginClassName" -> "plugin0",
          s"${PulsarFailoverSecondaryPrefix}0.authParams" -> "params0",
          s"${PulsarFailoverSecondaryPrefix}0.tlsTrustCertsFilePath" -> "/cert0.pem",
          s"${PulsarFailoverSecondaryPrefix}0.tlsTrustStorePath" -> "/truststore0.jks",
          s"${PulsarFailoverSecondaryPrefix}0.tlsTrustStorePassword" -> "password0",
          s"${PulsarFailoverSecondaryPrefix}1.serviceUrl" -> secondary1Url,
          s"${PulsarFailoverSecondaryPrefix}1.authPluginClassName" -> "plugin1",
          s"${PulsarFailoverSecondaryPrefix}1.authParams" -> "params1",
          s"${PulsarFailoverSecondaryPrefix}1.tlsTrustCertsFilePath" -> "/cert1.pem",
          s"${PulsarFailoverSecondaryPrefix}1.tlsTrustStorePath" -> "/truststore1.jks",
          s"${PulsarFailoverSecondaryPrefix}1.tlsTrustStorePassword" -> "password1"))
      .get

    assert(config.primaryServiceUrl === primaryUrl)
    assert(config.failoverDelay === Duration.ofMillis(5000))
    assert(config.switchBackDelay === Duration.ofMillis(10000))
    assert(config.checkInterval === Duration.ofMillis(15000))
    assert(config.policy === FailoverPolicy.ORDER)
    assert(
      config.secondaries === Seq(
        SecondaryClusterConfig(
          secondary0Url,
          Some("plugin0" -> "params0"),
          Some("/cert0.pem"),
          Some("/truststore0.jks"),
          Some("password0")),
        SecondaryClusterConfig(
          secondary1Url,
          Some("plugin1" -> "params1"),
          Some("/cert1.pem"),
          Some("/truststore1.jks"),
          Some("password1"))))
  }

  test("secondary auth plugin and params must be configured together") {
    val error = intercept[IllegalArgumentException] {
      PulsarFailoverConfig.fromParams(
        Map(
          PulsarFailoverPrimaryServiceUrlOptionKey -> primaryUrl,
          s"${PulsarFailoverSecondaryPrefix}0.serviceUrl" -> secondary0Url,
          s"${PulsarFailoverSecondaryPrefix}0.authPluginClassName" -> "plugin0"))
    }

    assert(error.getMessage.contains("authPluginClassName"))
    assert(error.getMessage.contains("authParams"))
  }

  test("stray failover options do not enable failover without primary service url") {
    assert(
      PulsarFailoverConfig
        .fromParams(
          Map(ServiceUrlOptionKey -> primaryUrl, PulsarFailoverDelayMsOptionKey -> "5000"))
        .isEmpty)
  }

  test("toServiceUrlProvider builds AutoClusterFailover provider") {
    val config = PulsarFailoverConfig(
      primaryUrl,
      Seq(
        SecondaryClusterConfig(
          secondary0Url,
          Some("org.apache.pulsar.client.impl.auth.AuthenticationToken" -> "token:secondary"),
          Some("/cert0.pem"),
          Some("/truststore0.jks"),
          Some("password0")),
        SecondaryClusterConfig(secondary1Url, None, None, None, None)),
      Duration.ofMillis(5000),
      Duration.ofMillis(10000),
      Duration.ofMillis(15000),
      FailoverPolicy.ORDER)

    val provider = PulsarFailoverConfig.toServiceUrlProvider(config)

    val failover = provider.asInstanceOf[org.apache.pulsar.client.impl.AutoClusterFailover]
    assert(failover.getPrimary === primaryUrl)
    assert(failover.getSecondary === Seq(secondary0Url, secondary1Url).asJava)
    assert(failover.getFailoverPolicy === FailoverPolicy.ORDER)
    assert(failover.getFailoverDelayNs === Duration.ofMillis(5000).toNanos)
    assert(failover.getSwitchBackDelayNs === Duration.ofMillis(10000).toNanos)
    assert(failover.getIntervalMs === 15000L)
    assert(failover.getSecondaryAuthentications.size() === 2)
    assert(failover.getSecondaryAuthentications.get(secondary0Url) !== null)
    assert(failover.getSecondaryAuthentications.get(secondary1Url) === null)
    assert(
      failover.getSecondaryTlsTrustCertsFilePaths.asScala.toMap ===
        Map(secondary0Url -> "/cert0.pem", secondary1Url -> null))
    assert(
      failover.getSecondaryTlsTrustStorePaths.asScala.toMap ===
        Map(secondary0Url -> "/truststore0.jks", secondary1Url -> null))
    assert(
      failover.getSecondaryTlsTrustStorePasswords.asScala.toMap ===
        Map(secondary0Url -> "password0", secondary1Url -> null))
  }
}
