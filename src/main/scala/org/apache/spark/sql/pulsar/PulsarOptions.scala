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
  val TopicPattern: String = "topicsPattern".toLowerCase

  val PartitionSuffix: String = TopicName.PARTITIONED_TOPIC_SUFFIX

  val TopicOptionKeys: Set[String] = Set(TopicSingle, TopicMulti, TopicPattern)

  val ServiceUrlOptionKey: String = "service.url"
  val AdminUrlOptionKey: String = "admin.url"
  val StartingOffsetsOptionKey: String = "startingOffsets".toLowerCase
  val StartingTime: String = "startingTime".toLowerCase
  val EndingOffsetsOptionKey: String = "endingOffsets".toLowerCase
  val SubscriptionPrefix: String = "subscriptionPrefix".toLowerCase
  val PredefinedSubscription: String = "predefinedSubscription".toLowerCase

  val PollTimeoutMS: String = "pollTimeoutMs".toLowerCase
  val FailOnDataLossOptionKey: String = "failOnDataLoss".toLowerCase

  val AuthPluginClassName: String = "authPluginClassName"
  val AuthParams: String = "authParams"
  val TlsTrustCertsFilePath: String = "tlsTrustCertsFilePath"
  val TlsAllowInsecureConnection: String = "tlsAllowInsecureConnection"
  val TlsHostnameVerificationEnable: String = "tlsHostnameVerificationEnable"

  val AllowDifferentTopicSchemas: String = "allowDifferentTopicSchemas".toLowerCase

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
