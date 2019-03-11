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

private[pulsar] object PulsarOptions {

  // options
  val TOPIC_OPTION_KEY = "topic"
  val SERVICE_URL_OPTION_KEY = "service.url"
  val TOPIC_SCHEMA_CLASS_OPTION_KEY = "topic.schema.class"

  val FILTERED_KEYS: Set[String] = Set(
    TOPIC_OPTION_KEY,
    SERVICE_URL_OPTION_KEY,
    TOPIC_SCHEMA_CLASS_OPTION_KEY
  )

  // attributes for sink
  val TOPIC_ATTRIBUTE_NAME: String = "topic"
  val KEY_ATTRIBUTE_NAME: String = "key"
  val VALUE_ATTRIBUTE_NAME: String = "value"

}
