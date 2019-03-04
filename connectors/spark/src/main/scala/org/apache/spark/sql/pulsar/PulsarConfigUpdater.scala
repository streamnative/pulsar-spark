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

import scala.collection.JavaConverters._

import org.apache.spark.internal.Logging

/**
 * Class to conveniently update pulsar config params, while logging the changes.
 */
private[pulsar] case class PulsarConfigUpdater(module: String, pulsarParams: Map[String, String])
    extends Logging {

  private val map = new ju.HashMap[String, Object](pulsarParams.asJava)

  def set(key: String, value: Object): this.type = {
    map.put(key, value)
    logDebug(s"$module: Set $key to $value, earlier value: ${pulsarParams.getOrElse(key, "")}")
    this
  }

  def setIfUnset(key: String, value: Object): this.type = {
    if (!map.containsKey(key)) {
      map.put(key, value)
      logDebug(s"$module: Set $key to $value")
    }
    this
  }

  def setAuthenticationConfigIfNeeded(): this.type = {
    // FIXME: not implemented yet
    this
  }

  def build(): ju.Map[String, Object] = map

}
