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

import scala.jdk.CollectionConverters._

import org.apache.spark.{ErrorClassesJsonReader, SparkThrowable}

private object PulsarExceptionsHelper {
  val errorClassesJsonReader: ErrorClassesJsonReader =
    new ErrorClassesJsonReader(
      Seq(getClass.getClassLoader.getResource("error/pulsar-error-classes.json")))
}

object PulsarExceptions {
  def pulsarProviderInvalidSaveMode(saveMode: String): PulsarIllegalArgumentException = {
    new PulsarIllegalArgumentException(
      errorClass = "PULSAR_PROVIDER_INVALID_SAVE_MODE",
      messageParameters = Map("saveMode" -> saveMode)
    )
  }

  def pulsarSinkUnsupportedDataType(dataType: String): PulsarIllegalArgumentException = {
    new PulsarIllegalArgumentException(
      errorClass = "PULSAR_SINK_UNSUPPORTED_DATA_TYPE",
      messageParameters = Map("dataType" -> dataType)
    )
  }

  def pulsarSinkInvalidSchema: PulsarIllegalArgumentException = {
    new PulsarIllegalArgumentException(
      errorClass = "PULSAR_SINK_INVALID_SCHEMA",
      messageParameters = Map()
    )
  }

  def pulsarSinkTopicAttributeNameMissing(
    topicAttributeName: String,
    topicSingle: String): PulsarIllegalArgumentException = {
    new PulsarIllegalArgumentException(
      errorClass = "PULSAR_SINK_TOPIC_ATTRIBUTE_NAME_MISSING",
      messageParameters = Map("topicAttributeName" -> topicAttributeName,
        "topicSingle" -> topicSingle)
    )
  }

  def pulsarSinkAttributeTypeMismatch(
    attributeName: String,
    expectedTypes: List[String]): PulsarIllegalArgumentException = {
    new PulsarIllegalArgumentException(
      errorClass = "PULSAR_SINK_ATTRIBUTE_TYPE_MISMATCH",
      messageParameters = Map("attributeName" -> attributeName,
        "expectedTypes" -> expectedTypes.mkString(","))
    )
  }

  def pulsarSinkIncompatibleSchema(
    topic: String,
    cause: Throwable): PulsarIllegalStateException = {
    new PulsarIllegalStateException(
      errorClass = "PULSAR_SINK_INCOMPATIBLE_SCHEMA",
      messageParameters = Map("topic"-> topic, "details" -> cause.getMessage),
      cause = cause
    )
  }

  def pulsarSinkProducerCreationFailure(
    topic: String,
    cause: Throwable): PulsarIllegalStateException = {
    new PulsarIllegalStateException(
      errorClass = "PULSAR_SINK_PRODUCER_CREATION_FAILURE",
      messageParameters = Map("topic"-> topic, "details" -> cause.getMessage),
      cause = cause
    )
  }
}

/**
 * Illegal state exception thrown with an error class.
 */
private[pulsar] class PulsarIllegalStateException(
  errorClass: String,
  messageParameters: Map[String, String],
  cause: Throwable = null)
  extends IllegalStateException(
    PulsarExceptionsHelper.errorClassesJsonReader.getErrorMessage(
      errorClass, messageParameters), cause)
    with SparkThrowable {

  override def getSqlState: String =
    PulsarExceptionsHelper.errorClassesJsonReader.getSqlState(errorClass)

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}


/**
 * Illegal argument exception thrown with an error class.
 */
private[pulsar] class PulsarIllegalArgumentException(
  errorClass: String,
  messageParameters: Map[String, String],
  cause: Throwable = null)
  extends IllegalArgumentException(
    PulsarExceptionsHelper.errorClassesJsonReader.getErrorMessage(
      errorClass, messageParameters), cause)
    with SparkThrowable {

  override def getSqlState: String =
    PulsarExceptionsHelper.errorClassesJsonReader.getSqlState(errorClass)

  override def getMessageParameters: java.util.Map[String, String] = messageParameters.asJava

  override def getCondition: String = errorClass
}
