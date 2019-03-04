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

import java.util.Locale
import java.{util => ju}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

/**
 * The provider class for all Pulsar readers and writers. It is designed such that it throws
 * IllegalArgumentException when the Pulsar Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[pulsar] class PulsarProvider extends DataSourceRegister
  with StreamSinkProvider {
  import PulsarOptions._
  import PulsarProvider._

  override def shortName(): String = "pulsar"

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    val pulsarTopic = parameters.get(TOPIC_OPTION_KEY).map(_.trim)
    val pulsarServiceUrl = parameters.get(SERVICE_URL_OPTION_KEY).map(_.trim)
    val specifiedPulsarParams = pulsarParamsForProducer(parameters)

    if (pulsarTopic.isEmpty) {
      throw new IllegalArgumentException(
        "Pulsar option '" + TOPIC_OPTION_KEY + "' is not defined")
    }

    if (pulsarServiceUrl.isEmpty) {
      throw new IllegalArgumentException(
        "Pulsar option '" + SERVICE_URL_OPTION_KEY + "' is not defined")
    }

    new PulsarSink(
      sqlContext,
      specifiedPulsarParams,
      pulsarServiceUrl.get,
      pulsarTopic.get
    )
  }
}

private[pulsar] object PulsarProvider extends Logging {
  import PulsarOptions._

  private[pulsar] def pulsarParamsForProducer(
      parameters: Map[String, String]): ju.Map[String, Object] = {
    val caseInsensitiveParams = parameters.map {
      case (k, v) => (k.toLowerCase(Locale.ROOT), v)
    }
    if (caseInsensitiveParams.contains(TOPIC_SCHEMA_CLASS_OPTION_KEY)) {
      throw new IllegalArgumentException(
        "Pulsar option '" + TOPIC_SCHEMA_CLASS_OPTION_KEY + "' is not supported"
          + " as value are serialized with Schema.BYTES")
    }

    val specifiedPulsarParams = convertToSpecifiedParams(parameters)

    PulsarConfigUpdater("producer", specifiedPulsarParams)
      .setAuthenticationConfigIfNeeded()
      .build()
  }

  private def convertToSpecifiedParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.toLowerCase(Locale.ROOT).startsWith("spark.pulsar."))
      .map { k => k.drop(13).toString -> parameters(k) }
      .toMap
  }

}
