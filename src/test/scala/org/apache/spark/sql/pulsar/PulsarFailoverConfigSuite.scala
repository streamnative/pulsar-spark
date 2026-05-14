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
      PulsarFailoverConfig.fromParams(Map(
        PulsarFailoverPrimaryServiceUrlOptionKey -> primaryUrl,
        s"${PulsarFailoverSecondaryPrefix}0.serviceUrl" -> secondary0Url,
        s"${PulsarFailoverSecondaryPrefix}2.serviceUrl" -> secondary1Url))
    }

    assert(error.getMessage.contains("continuous"))
    assert(error.getMessage.contains("missing indexes: 1"))
  }

  test("non-positive duration fails") {
    val error = intercept[IllegalArgumentException] {
      PulsarFailoverConfig.fromParams(Map(
        PulsarFailoverPrimaryServiceUrlOptionKey -> primaryUrl,
        s"${PulsarFailoverSecondaryPrefix}0.serviceUrl" -> secondary0Url,
        PulsarFailoverDelayMsOptionKey -> "0"))
    }

    assert(error.getMessage.contains(PulsarFailoverDelayMsOptionKey))
    assert(error.getMessage.contains("positive"))
  }

  test("full config parses durations, policy, auth and tls per secondary") {
    val config = PulsarFailoverConfig.fromParams(Map(
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
      s"${PulsarFailoverSecondaryPrefix}1.tlsTrustStorePassword" -> "password1")).get

    assert(config.primaryServiceUrl === primaryUrl)
    assert(config.failoverDelay === Duration.ofMillis(5000))
    assert(config.switchBackDelay === Duration.ofMillis(10000))
    assert(config.checkInterval === Duration.ofMillis(15000))
    assert(config.policy === FailoverPolicy.ORDER)
    assert(config.secondaries === Seq(
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
      PulsarFailoverConfig.fromParams(Map(
        PulsarFailoverPrimaryServiceUrlOptionKey -> primaryUrl,
        s"${PulsarFailoverSecondaryPrefix}0.serviceUrl" -> secondary0Url,
        s"${PulsarFailoverSecondaryPrefix}0.authPluginClassName" -> "plugin0"))
    }

    assert(error.getMessage.contains("authPluginClassName"))
    assert(error.getMessage.contains("authParams"))
  }

  test("toServiceUrlProvider builds AutoClusterFailover provider") {
    val config = PulsarFailoverConfig(
      primaryUrl,
      Seq(SecondaryClusterConfig(secondary0Url, None, None, None, None)),
      Duration.ofMillis(5000),
      Duration.ofMillis(10000),
      Duration.ofMillis(15000),
      FailoverPolicy.ORDER)

    val provider = PulsarFailoverConfig.toServiceUrlProvider(config)

    assert(provider.getClass.getName.contains("AutoClusterFailover"))
    assert(provider.getServiceUrl === primaryUrl)
  }
}
