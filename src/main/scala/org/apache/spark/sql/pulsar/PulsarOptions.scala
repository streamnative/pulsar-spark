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

import java.util.Locale

import org.apache.pulsar.common.naming.TopicName

// All options should be lowercase to simplify parameter matching
private[pulsar] object PulsarOptions {

  // option key prefix for different modules

  val PulsarAdminOptionKeyPrefix: String = "pulsar.admin."
  val PulsarClientOptionKeyPrefix: String = "pulsar.client."
  val PulsarProducerOptionKeyPrefix: String = "pulsar.producer."
  val PulsarReaderOptionKeyPrefix: String = "pulsar.reader."

  // options

  val TopicSingle: String = "topic"
  val TopicMulti: String = "topics"
  val TopicPattern: String = "topicsPattern".toLowerCase(Locale.ROOT)

  val PartitionSuffix: String = TopicName.PARTITIONED_TOPIC_SUFFIX

  val TopicOptionKeys: Set[String] = Set(TopicSingle, TopicMulti, TopicPattern)

  val MaxEntriesPerTrigger = "maxentriespertrigger"

  val ServiceUrlOptionKey: String = "service.url"
  val AdminUrlOptionKey: String = "admin.url"
  val StartingOffsetsOptionKey: String = "startingOffsets".toLowerCase(Locale.ROOT)
  val StartingTime: String = "startingTime".toLowerCase(Locale.ROOT)
  val EndingTime: String = "endingTime".toLowerCase(Locale.ROOT)
  val EndingOffsetsOptionKey: String = "endingOffsets".toLowerCase(Locale.ROOT)
  val StartOptionKey: String = "startOptionKey".toLowerCase(Locale.ROOT)
  val EndOptionKey: String = "endOptionKey".toLowerCase(Locale.ROOT)
  val SubscriptionPrefix: String = "subscriptionPrefix".toLowerCase(Locale.ROOT)
  val PredefinedSubscription: String = "predefinedSubscription".toLowerCase(Locale.ROOT)

  val PollTimeoutMS: String = "pollTimeoutMs".toLowerCase(Locale.ROOT)
  val FailOnDataLossOptionKey: String = "failOnDataLoss".toLowerCase(Locale.ROOT)

  val AuthPluginClassName: String = "authPluginClassName"
  val AuthParams: String = "authParams"
  val TlsTrustCertsFilePath: String = "tlsTrustCertsFilePath"
  val TlsAllowInsecureConnection: String = "tlsAllowInsecureConnection"
  val TlsHostnameVerificationEnable: String = "tlsHostnameVerificationEnable"

  val AllowDifferentTopicSchemas: String = "allowDifferentTopicSchemas".toLowerCase(Locale.ROOT)

  val InstructionForFailOnDataLossFalse: String =
    """
      |Some data may have been lost because they are not available in Pulsar any more; either the
      | data was aged out by Pulsar or the topic may have been deleted before all the data in the
      | topic was processed. If you want your streaming query to fail on such cases, set the source
      | option "failOnDataLoss" to "true".
    """.stripMargin

  val InstructionForFailOnDataLossTrue: String =
    """
      |Some data may have been lost because they are not available in Pulsar any more; either the
      | data was aged out by Pulsar or the topic may have been deleted before all the data in the
      | topic was processed. If you don't want your streaming query to fail on such cases, set the
      | source option "failOnDataLoss" to "false".
    """.stripMargin

  val TopicSchemaClassOptionKey: String = "topic.schema.class"

  val FilteredKeys: Set[String] = Set(TopicSingle, ServiceUrlOptionKey, TopicSchemaClassOptionKey)

  val TopicAttributeName: String = "__topic"
  val KeyAttributeName: String = "__key"
  val MessageIdName: String = "__messageId"
  val PublishTimeName: String = "__publishTime"
  val EventTimeName: String = "__eventTime"
  val MessagePropertiesName: String = "__messageProperties"

  val MetaFieldNames: Set[String] = Set(
    TopicAttributeName,
    KeyAttributeName,
    MessageIdName,
    PublishTimeName,
    EventTimeName,
    MessagePropertiesName)
}
