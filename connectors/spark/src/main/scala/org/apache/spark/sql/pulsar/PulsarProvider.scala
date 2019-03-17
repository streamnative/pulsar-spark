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

import java.util.Optional
import java.{util => ju}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SQLContext, SaveMode, SparkSession}
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, StreamWriteSupport}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType

import scala.collection.JavaConverters._

/**
 * The provider class for all Pulsar readers and writers. It is designed such that it throws
 * IllegalArgumentException when the Pulsar Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[pulsar] class PulsarProvider extends DataSourceRegister
  with StreamSourceProvider
  with StreamSinkProvider
  with RelationProvider
  with CreatableRelationProvider
  with StreamWriteSupport
  with ContinuousReadSupport
  with Logging {

  import PulsarOptions._
  import PulsarProvider._

  override def shortName(): String = "pulsar"

  private def validateCommonOptions(parameters: Map[String, String]): Unit = {
    if (!parameters.contains(s"$SERVICE_URL_OPTION_KEY")) {
      throw new IllegalArgumentException(
        s"Option '${SERVICE_URL_OPTION_KEY}' must be specified for " +
          s"configuring Pulsar consumer")
    }
  }

  private def validateStreamCommonOptions(parameters: Map[String, String]) = {
    validateCommonOptions(parameters)
  }

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {
    validateStreamCommonOptions(parameters)
    require(schema.isEmpty, "Pulsar source has a fixed schema and cannot be set with a custom one")
    (shortName(), PulsarReader.pulsarSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = ???


  override def createContinuousReader(
      schema: Optional[StructType],
      metadataPath: String,
      options: DataSourceOptions): PulsarContinuousReader = {
    val parameters = options.asMap().asScala.toMap

    // validate common params
    val sparkPulsarCommonParams = convertToSpecifiedSparkPulsarCommonParams(parameters)
    validateStreamCommonOptions(sparkPulsarCommonParams)
    val serviceUrl = sparkPulsarCommonParams.get(SERVICE_URL_OPTION_KEY)

    // validate source params
    val sparkPulsarSourceParams = convertToSpecifiedSparkPulsarSourceParams(parameters)
    val startingStreamOffsets = getPulsarOffset(
      sparkPulsarSourceParams,
      STARTING_OFFSETS_OPTION_KEY)

    // pulsar client params
    var pulsarClientParams = convertToSpecifiedPulsarClientParams(parameters)
    // add `serviceUrl` back to client params
    pulsarClientParams += (SERVICE_URL_OPTION_KEY -> serviceUrl.get)

    // pulsar reader params
    val pulsarReaderParams = convertToSpecifiedPulsarReaderParams(parameters)

    new PulsarContinuousReader(
      paramsToPulsarConf("pulsar.client", pulsarClientParams),
      paramsToPulsarConf("pulsar.reader", pulsarReaderParams),
      startingStreamOffsets
    )
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {

    val parsedConf = parsePulsarParmsForProducer(parameters)

    new PulsarSink(
      sqlContext,
      parsedConf._1,
      parsedConf._2,
      parsedConf._3
    )
  }

  private def parsePulsarParmsForProducer(
      parameters: Map[String, String]): (ju.Map[String, Object], ju.Map[String, Object], String) = {
    val serviceUrl = parameters.get(s"${SPARK_PULSAR_COMMON_OPTION_KEY_PREFIX}${SERVICE_URL_OPTION_KEY}").map(_.trim)
    val topic = parameters.get(s"${SPARK_PULSAR_SINK_OPTION_KEY_PREFIX}${TOPIC_OPTION_KEY}").map(_.trim)

    if (topic.isEmpty) {
      throw new IllegalArgumentException(
        s"Pulsar option '${SPARK_PULSAR_SINK_OPTION_KEY_PREFIX}${TOPIC_OPTION_KEY}' is not defined")
    }

    if (serviceUrl.isEmpty) {
      throw new IllegalArgumentException(
        s"Pulsar option '${SPARK_PULSAR_COMMON_OPTION_KEY_PREFIX}${SERVICE_URL_OPTION_KEY}' is not defined")
    }

    // pulsar client params
    var pulsarClientParams: Map[String, String] = convertToSpecifiedPulsarClientParams(parameters)
    // add `serviceUrl` back to client params
    pulsarClientParams += (SERVICE_URL_OPTION_KEY -> serviceUrl.get)

    // pulsar producer params
    val pulsarProducerParams = convertToSpecifiedPulsarProducerParams(parameters)

    (
      paramsToPulsarConf("pulsar.client", pulsarClientParams),
      paramsToPulsarConf("pulsar.producer", pulsarProducerParams),
      topic.get
    )
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
      parsedConf._1,
      parsedConf._2,
      parsedConf._3)

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

  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceOptions): StreamWriter = {
    import scala.collection.JavaConverters._

    val spark = SparkSession.getActiveSession.get
    val parsedConf = parsePulsarParmsForProducer(options.asMap().asScala.toMap)

    new PulsarStreamWriter(
      schema, parsedConf._1, parsedConf._2, parsedConf._3)
  }

}

private[pulsar] object PulsarProvider extends Logging {
  import PulsarOptions._

  private def convertToSpecifiedSparkPulsarCommonParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.startsWith(SPARK_PULSAR_COMMON_OPTION_KEY_PREFIX))
      .map { k => k.drop(SPARK_PULSAR_COMMON_OPTION_KEY_PREFIX.length).toString -> parameters(k) }
      .toMap
  }

  private def convertToSpecifiedSparkPulsarSourceParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.startsWith(SPARK_PULSAR_SOURCE_OPTION_KEY_PREFIX))
      .map { k => k.drop(SPARK_PULSAR_SOURCE_OPTION_KEY_PREFIX.length).toString -> parameters(k) }
      .toMap
  }

  private def convertToSpecifiedSparkPulsarSinkParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.startsWith(SPARK_PULSAR_SINK_OPTION_KEY_PREFIX))
      .map { k => k.drop(SPARK_PULSAR_SINK_OPTION_KEY_PREFIX.length).toString -> parameters(k) }
      .toMap
  }

  private def convertToSpecifiedPulsarClientParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.startsWith(PULSAR_CLIENT_OPTION_KEY_PREFIX))
      .map { k => k.drop(PULSAR_CLIENT_OPTION_KEY_PREFIX.length).toString -> parameters(k) }
      .toMap
  }

  private def convertToSpecifiedPulsarProducerParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.startsWith(PULSAR_PRODUCER_OPTION_KEY_PREFIX))
      .map { k => k.drop(PULSAR_PRODUCER_OPTION_KEY_PREFIX.length).toString -> parameters(k) }
      .toMap
  }

  private def convertToSpecifiedPulsarConsumerParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.startsWith(PULSAR_CONSUMER_OPTION_KEY_PREFIX))
      .map { k => k.drop(PULSAR_CONSUMER_OPTION_KEY_PREFIX.length).toString -> parameters(k) }
      .toMap
  }

  private def convertToSpecifiedPulsarReaderParams(parameters: Map[String, String]): Map[String, String] = {
    parameters
      .keySet
      .filter(_.startsWith(PULSAR_READER_OPTION_KEY_PREFIX))
      .map { k => k.drop(PULSAR_READER_OPTION_KEY_PREFIX.length).toString -> parameters(k) }
      .toMap
  }

  def getPulsarOffset(
      params: Map[String, String],
      offsetOptionKey: String): PulsarSourceOffset = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(json) => PulsarSourceOffset(JsonUtils.topicOffsets(json))
      case None => throw new IllegalArgumentException(
        s"Pulsar option '${SPARK_PULSAR_SOURCE_OPTION_KEY_PREFIX}${offsetOptionKey}' is not specified"
      )
    }
  }

  def paramsToPulsarConf(
      module: String,
      params: Map[String, String]): ju.Map[String, Object] = {
    PulsarConfigUpdater(module, params)
      .rebuild()
  }

}
