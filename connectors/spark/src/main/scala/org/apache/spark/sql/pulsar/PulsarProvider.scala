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
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode}
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import scala.collection.JavaConverters._

/**
 * The provider class for all Pulsar readers and writers. It is designed such that it throws
 * IllegalArgumentException when the Pulsar Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[pulsar] class PulsarProvider extends DataSourceRegister
  with StreamSinkProvider
  with RelationProvider
  with CreatableRelationProvider
  with Logging {

  import PulsarOptions._
  import PulsarProvider._

  override def shortName(): String = "pulsar"

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {

    val parsedConf = parsePulsarParmsForProducer(parameters)

    new PulsarSink(
      sqlContext,
      parsedConf._3,
      parsedConf._1,
      parsedConf._2
    )
  }

  private def parsePulsarParmsForProducer(
      parameters: Map[String, String]): (String, String, ju.Map[String, Object]) = {
    val serviceUrl = parameters.get(s"spark.pulsar.${SERVICE_URL_OPTION_KEY}").map(_.trim)
    val topic = parameters.get(s"spark.pulsar.${TOPIC_OPTION_KEY}").map(_.trim)

    if (topic.isEmpty) {
      throw new IllegalArgumentException(
        "Pulsar option '" + TOPIC_OPTION_KEY + "' is not defined")
    }

    if (serviceUrl.isEmpty) {
      throw new IllegalArgumentException(
        "Pulsar option '" + SERVICE_URL_OPTION_KEY + "' is not defined")
    }

    val specifiedPulsarParams =
      new PulsarConfigUpdater(
        "provider",
        pulsarParamsForProducer(parameters)
          .asScala.toMap,
        PulsarOptions.FILTERED_KEYS
      ).rebuild()

    (serviceUrl.get, topic.get, specifiedPulsarParams)
  }

  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    throw new UnsupportedOperationException("Not implemented yet")
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(s"Save mode $mode not allowed for Pulsar. "
          + s"Allowed save mode are ${SaveMode.Append} and "
          + s"${SaveMode.ErrorIfExists} (default).")
      case _ => // good
    }

    val parsedConf = parsePulsarParmsForProducer(parameters)

    PulsarWriter.write(
      sqlContext.sparkSession,
      data.queryExecution,
      parsedConf._3,
      parsedConf._1,
      parsedConf._2)

    /**
     * This method is suppose to return a relation the data that was written.
     * Currently we haven't supported schema yet. Therefore, we return an empty
     * base relation for now.
     */
    new BaseRelation {
      override def sqlContext: SQLContext = unsupportedException
      // FIXME: integration with pulsar schema
      override def schema: StructType = unsupportedException
      override def needConversion: Boolean = unsupportedException
      override def sizeInBytes: Long = unsupportedException
      override def unhandledFilters(filters: Array[Filter]): Array[Filter] = unsupportedException

      private def unsupportedException =
        throw new UnsupportedOperationException("BaseRelation from Kafka write " +
          "operation is not usable.")
    }
  }
}

private[pulsar] object PulsarProvider extends Logging {
  import PulsarOptions._

  private[pulsar] def pulsarParamsForProducer(
      parameters: Map[String, String]): ju.Map[String, Object] = {
    if (parameters.contains(s"spark.pulsar.${TOPIC_SCHEMA_CLASS_OPTION_KEY}")) {
      throw new IllegalArgumentException(
        "Pulsar option '" + TOPIC_SCHEMA_CLASS_OPTION_KEY + "' is not supported"
          + " as value are serialized with Schema.BYTES")
    }

    val specifiedPulsarParams = convertToSpecifiedParams(parameters)
    logInfo(s"parameters : ${parameters} => specified pulsar parameters : ${specifiedPulsarParams}")

    PulsarConfigUpdater("producer", specifiedPulsarParams)
      .setAuthenticationConfigIfNeeded()
      .rebuild()
  }

  private def convertToSpecifiedParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.startsWith("spark.pulsar."))
      .map { k => k.drop(13).toString -> parameters(k) }
      .toMap
  }

}
