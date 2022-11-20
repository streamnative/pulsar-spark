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

import scala.collection.JavaConverters._

import org.apache.pulsar.client.admin.PulsarAdmin

import org.apache.spark.sql.pulsar.PulsarOptions.{AuthParams, AuthPluginClassName}

object AdminUtils {

  def buildAdmin(adminUrl: String, config: ju.Map[String, Object]): PulsarAdmin = {
    val clientConf =
      PulsarConfigUpdater("pulsarClientCache", config.asScala.toMap, PulsarOptions.FilteredKeys)
        .rebuild()

    val builder = PulsarAdmin
      .builder()
      .loadConf(clientConf)
      .serviceHttpUrl(adminUrl)

    // Set Pulsar authentication. This couldn't be load by the loadConf directly.
    Option(clientConf.get(AuthPluginClassName)).foreach(pluginClasName => {
      builder.authentication(pluginClasName.toString, clientConf.get(AuthParams).toString)
    })

    builder.build()
  }
}
