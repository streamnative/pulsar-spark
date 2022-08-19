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

import org.apache.pulsar.common.naming.TopicName

// All options should be lowercase to simplify parameter matching
private[pulsar] object PulsarOptions {

  // option key prefix for different modules
  val PulsarAdminOptionKeyPrefix = "pulsar.admin."
  val PulsarClientOptionKeyPrefix = "pulsar.client."
  val PulsarProducerOptionKeyPrefix = "pulsar.producer."
  val PulsarConsumerOptionKeyPrefix = "pulsar.consumer."
  val PulsarReaderOptionKeyPrefix = "pulsar.reader."

  // options

  val TopicSingle = "topic"
  val TopicMulti = "topics"
  val TopicPattern = "topicspattern"

  val PartitionSuffix: String = TopicName.PARTITIONED_TOPIC_SUFFIX

  val TopicOptionKeys = Set(
    TopicSingle,
    TopicMulti,
    TopicPattern
  )

  val ServiceUrlOptionKey = "service.url"
  val AdminUrlOptionKey = "admin.url"
  val StartingOffsetsOptionKey = "startingoffsets"
  val StartingTime = "startingtime"
  val EndingOffsetsOptionKey = "endingoffsets"
  val SubscriptionPrefix = "subscriptionprefix"
  val PredefinedSubscription = "predefinedsubscription"

  val PollTimeoutMS = "polltimeoutms"
  val FailOnDataLossOptionKey = "failondataloss"

  val AuthPluginClassName = "authPluginClassName"
  val AuthParams = "authParams"
  val TlsTrustCertsFilePath = "tlsTrustCertsFilePath"
  val TlsAllowInsecureConnection = "tlsAllowInsecureConnection"
  val UseTls = "useTls"
  val TlsHostnameVerificationEnable = "tlsHostnameVerificationEnable"

  val AllowDifferentTopicSchemas = "allowdifferenttopicschemas"

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

  val TopicSchemaClassOptionKey = "topic.schema.class"

  val FilteredKeys: Set[String] =
    Set(TopicSingle, ServiceUrlOptionKey, TopicSchemaClassOptionKey)

  val TopicAttributeName: String = "__topic"
  val KeyAttributeName: String = "__key"
  val MessageIdName: String = "__messageId"
  val PublishTimeName: String = "__publishTime"
  val EventTimeName: String = "__eventTime"
  val MessagePropertiesName: String = "__messageProperties"

  val MetaFieldNames = Set(
    TopicAttributeName,
    KeyAttributeName,
    MessageIdName,
    PublishTimeName,
    EventTimeName,
    MessagePropertiesName
  )
}
