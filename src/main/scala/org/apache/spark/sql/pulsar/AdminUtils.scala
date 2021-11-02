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

import java.{util => ju}

import org.apache.pulsar.client.admin.PulsarAdmin

object AdminUtils {

  import PulsarOptions._

  def buildAdmin(adminUrl: String, clientConf: ju.Map[String, Object]): PulsarAdmin = {
    val builder = PulsarAdmin.builder().serviceHttpUrl(adminUrl)

    if (clientConf.containsKey(AuthPluginClassName)) {
      builder.authentication(
        clientConf.get(AuthPluginClassName).toString, clientConf.get(AuthParams).toString)
    }

    if (clientConf.containsKey(TlsAllowInsecureConnection)) {
      builder.allowTlsInsecureConnection(
        clientConf.get(TlsAllowInsecureConnection).toString.toBoolean)
    }

    if (clientConf.containsKey(TlsHostnameVerificationEnable)) {
      builder.enableTlsHostnameVerification(
        clientConf.get(TlsHostnameVerificationEnable).toString.toBoolean)
    }

    if (clientConf.containsKey(TlsTrustCertsFilePath)) {
      builder.tlsTrustCertsFilePath(clientConf.get(TlsTrustCertsFilePath).toString)
    }

    builder.build()
  }

}
