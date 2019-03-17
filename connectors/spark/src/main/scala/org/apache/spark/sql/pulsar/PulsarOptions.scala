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

  // option key prefix
  val PULSAR_CLIENT_OPTION_KEY_PREFIX = "pulsar.client."
  val PULSAR_PRODUCER_OPTION_KEY_PREFIX = "pulsar.producer."
  val PULSAR_CONSUMER_OPTION_KEY_PREFIX = "pulsar.consumer."
  val PULSAR_READER_OPTION_KEY_PREFIX = "pulsar.reader."
  val SPARK_PULSAR_COMMON_OPTION_KEY_PREFIX = "spark.pulsar.common."
  val SPARK_PULSAR_SINK_OPTION_KEY_PREFIX = "spark.pulsar.sink."
  val SPARK_PULSAR_SOURCE_OPTION_KEY_PREFIX = "spark.pulsar.source."

  // options

  val STRATEGY_OPTION_KEY_SUBSCRIBE = "subscribe"
  val STRATEGY_OPTION_KEY_SUBSCRIBEPATTERN = "subscribepattern"
  val STRATEGY_OPTION_KEY_ASSIGN = "assign"

  val STRATEGY_OPTION_KEYS = Set(
    STRATEGY_OPTION_KEY_SUBSCRIBE,
    STRATEGY_OPTION_KEY_SUBSCRIBEPATTERN,
    STRATEGY_OPTION_KEY_ASSIGN
  )

  val STARTING_OFFSETS_OPTION_KEY = "startingoffsets"
  val ENDING_OFFSETS_OPTION_KEY = "endingoffsets"
  val FAIL_ON_DATA_LOSS_OPTION_KEY = "failondataloss"
  val MIN_PARTITIONS_OPTION_KEY = "minpartitions"

  val TOPIC_OPTION_KEY = "topic"
  val SUBSCRIPTION_OPTION_KEY = "subscription"
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
