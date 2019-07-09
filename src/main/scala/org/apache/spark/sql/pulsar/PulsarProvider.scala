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
import java.util.{Locale, Optional, UUID}

import scala.collection.JavaConverters._

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.common.naming.TopicName

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.sources._
import org.apache.spark.sql.sources.v2.{ContinuousReadSupport, DataSourceOptions, MicroBatchReadSupport, StreamWriteSupport}
import org.apache.spark.sql.sources.v2.reader.streaming.MicroBatchReader
import org.apache.spark.sql.sources.v2.writer.streaming.StreamWriter
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * The provider class for all Pulsar readers and writers. It is designed such that it throws
 * IllegalArgumentException when the Pulsar Dataset is created, so that it can catch
 * missing options even before the query is started.
 */
private[pulsar] class PulsarProvider
    extends DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with RelationProvider
    with CreatableRelationProvider
    with StreamWriteSupport
    with ContinuousReadSupport
    with MicroBatchReadSupport
    with Logging {

  import PulsarOptions._
  import PulsarProvider._

  override def shortName(): String = "pulsar"

  override def sourceSchema(
      sqlContext: SQLContext,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): (String, StructType) = {

    val caseInsensitiveParams = validateStreamOptions(parameters)
    val confs = prepareConfForConsumer(parameters)

    val subscriptionNamePrefix = s"spark-pulsar-${UUID.randomUUID}"
    val inferredSchema = Utils.tryWithResource(
      new PulsarMetadataReader(
        confs._3,
        confs._4,
        confs._1,
        subscriptionNamePrefix,
        caseInsensitiveParams)) { reader =>
      reader.getAndCheckCompatible(schema)
    }
    (shortName(), inferredSchema)
  }

  override def createSource(
      sqlContext: SQLContext,
      metadataPath: String,
      schema: Option[StructType],
      providerName: String,
      parameters: Map[String, String]): Source = {
    val caseInsensitiveParams = validateStreamOptions(parameters)
    val confs = prepareConfForConsumer(parameters)

    val subscriptionNamePrefix = s"spark-pulsar-${UUID.randomUUID}-${metadataPath.hashCode}"
    val metadataReader = new PulsarMetadataReader(
      confs._3,
      confs._4,
      confs._1,
      subscriptionNamePrefix,
      caseInsensitiveParams)

    metadataReader.getAndCheckCompatible(schema)

    // start from latest offset if not specified to be consistent with Pulsar source
    val offset = metadataReader.offsetForEachTopic(
      caseInsensitiveParams,
      STARTING_OFFSETS_OPTION_KEY,
      LatestOffset)
    metadataReader.setupCursor(offset)

    new PulsarSource(
      sqlContext,
      metadataReader,
      confs._1,
      confs._2,
      metadataPath,
      offset,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions
    )
  }

  override def createMicroBatchReader(
      schema: Optional[StructType],
      metadataPath: String,
      options: DataSourceOptions): MicroBatchReader = {
    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = validateStreamOptions(parameters)
    val confs = prepareConfForConsumer(parameters)

    val subscriptionNamePrefix = s"spark-pulsar-${UUID.randomUUID}-${metadataPath.hashCode}"
    val metadataReader = new PulsarMetadataReader(
      confs._3,
      confs._4,
      confs._1,
      subscriptionNamePrefix,
      caseInsensitiveParams)

    metadataReader.getAndCheckCompatible(schema)

    // start from latest offset if not specified to be consistent with Pulsar source
    val offset: SpecificPulsarOffset = metadataReader.offsetForEachTopic(
      caseInsensitiveParams,
      STARTING_OFFSETS_OPTION_KEY,
      LatestOffset)
    metadataReader.setupCursor(offset)

    new PulsarMicroBatchReader(
      metadataReader,
      confs._1,
      confs._2,
      metadataPath,
      offset,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions
    )
  }

  override def createContinuousReader(
      schema: Optional[StructType],
      metadataPath: String,
      options: DataSourceOptions): PulsarContinuousReader = {
    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = validateStreamOptions(parameters)
    val confs = prepareConfForConsumer(parameters)

    val subscriptionNamePrefix = s"spark-pulsar-${UUID.randomUUID}-${metadataPath.hashCode}"
    val metadataReader = new PulsarMetadataReader(
      confs._3,
      confs._4,
      confs._1,
      subscriptionNamePrefix,
      caseInsensitiveParams)

    metadataReader.getAndCheckCompatible(schema)

    val offset = metadataReader.offsetForEachTopic(
      caseInsensitiveParams,
      STARTING_OFFSETS_OPTION_KEY,
      LatestOffset)
    metadataReader.setupCursor(offset)

    new PulsarContinuousReader(
      metadataReader,
      confs._1,
      confs._2,
      offset,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions)
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val caseInsensitiveParams = validateBatchOptions(parameters)

    val subscriptionNamePrefix = s"spark-pulsar-batch-${UUID.randomUUID}"

    val confs = prepareConfForConsumer(parameters)
    val (start, end, schema, pSchema) = Utils.tryWithResource(
      new PulsarMetadataReader(
        confs._3,
        confs._4,
        confs._1,
        subscriptionNamePrefix,
        caseInsensitiveParams)) { reader =>
      val startingOffset = reader.offsetForEachTopic(
        caseInsensitiveParams,
        STARTING_OFFSETS_OPTION_KEY,
        EarliestOffset)
      val endingOffset =
        reader.offsetForEachTopic(caseInsensitiveParams, ENDING_OFFSETS_OPTION_KEY, LatestOffset)

      val pulsarSchema = reader.getPulsarSchema()
      val schema = SchemaUtils.pulsarSourceSchema(pulsarSchema)
      (startingOffset, endingOffset, schema, pulsarSchema)
    }

    new PulsarRelation(
      sqlContext,
      schema,
      new SchemaInfoSerializable(pSchema),
      confs._4,
      confs._1,
      confs._2,
      start,
      end,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions
    )
  }

  override def createRelation(
      sqlContext: SQLContext,
      mode: SaveMode,
      parameters: Map[String, String],
      data: DataFrame): BaseRelation = {
    mode match {
      case SaveMode.Overwrite | SaveMode.Ignore =>
        throw new AnalysisException(
          s"Save mode $mode not allowed for Pulsar. "
            + s"Allowed save mode are ${SaveMode.Append} and "
            + s"${SaveMode.ErrorIfExists} (default).")
      case _ => // good
    }

    val caseInsensitiveParams = validateSinkOptions(parameters)

    val parsedConf = prepareConfForProducer(caseInsensitiveParams)
    PulsarSinks.validateQuery(data.schema.toAttributes, parsedConf._3)

    PulsarSinks.write(
      sqlContext.sparkSession,
      data.queryExecution,
      parsedConf._1,
      parsedConf._2,
      parsedConf._3,
      parsedConf._4
    )

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
        throw new UnsupportedOperationException(
          "BaseRelation from Pulsar write " +
            "operation is not usable.")
    }
  }

  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {

    val caseInsensitiveParams = validateSinkOptions(parameters)

    val parsedConf = prepareConfForProducer(caseInsensitiveParams)

    new PulsarSink(
      sqlContext,
      parsedConf._1,
      parsedConf._2,
      parsedConf._3,
      parsedConf._4
    )
  }

  override def createStreamWriter(
      queryId: String,
      schema: StructType,
      mode: OutputMode,
      options: DataSourceOptions): StreamWriter = {

    import scala.collection.JavaConverters._
    val parameters = options.asMap().asScala.toMap
    val caseInsensitiveParams = validateSinkOptions(parameters)

    val parsedConf = prepareConfForProducer(caseInsensitiveParams)
    PulsarSinks.validateQuery(schema.toAttributes, parsedConf._3)

    new PulsarStreamWriter(schema, parsedConf._1, parsedConf._2, parsedConf._3, parsedConf._4)
  }
}

private[pulsar] object PulsarProvider extends Logging {
  import PulsarOptions._

  private def getClientParams(parameters: Map[String, String]): Map[String, String] = {
    parameters.keySet
      .filter(_.startsWith(PULSAR_CLIENT_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_CLIENT_OPTION_KEY_PREFIX.length).toString -> parameters(k)
      }
      .toMap
  }

  private def getProducerParams(parameters: Map[String, String]): Map[String, String] = {
    parameters.keySet
      .filter(_.startsWith(PULSAR_PRODUCER_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_PRODUCER_OPTION_KEY_PREFIX.length).toString -> parameters(k)
      }
      .toMap
  }

  private def getConsumerParams(parameters: Map[String, String]): Map[String, String] = {
    parameters.keySet
      .filter(_.startsWith(PULSAR_CONSUMER_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_CONSUMER_OPTION_KEY_PREFIX.length).toString -> parameters(k)
      }
      .toMap
  }

  private def getReaderParams(parameters: Map[String, String]): Map[String, String] = {
    parameters.keySet
      .filter(_.startsWith(PULSAR_READER_OPTION_KEY_PREFIX))
      .map { k =>
        k.drop(PULSAR_READER_OPTION_KEY_PREFIX.length).toString -> parameters(k)
      }
      .toMap
  }

  def getPulsarOffset(
      params: Map[String, String],
      offsetOptionKey: String,
      defaultOffsets: PulsarOffset): PulsarOffset = {
    params.get(offsetOptionKey).map(_.trim) match {
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "latest" =>
        LatestOffset
      case Some(offset) if offset.toLowerCase(Locale.ROOT) == "earliest" =>
        EarliestOffset
      case Some(json) =>
        SpecificPulsarOffset(JsonUtils.topicOffsets(json))
      case None => defaultOffsets
    }
  }

  def paramsToPulsarConf(module: String, params: Map[String, String]): ju.Map[String, Object] = {
    PulsarConfigUpdater(module, params).rebuild()
  }

  private def getServiceUrl(parameters: Map[String, String]): String = {
    parameters.get(SERVICE_URL_OPTION_KEY).get
  }

  private def getAdminUrl(parameters: Map[String, String]): String = {
    parameters.get(ADMIN_URL_OPTION_KEY).get
  }

  private def failOnDataLoss(caseInsensitiveParams: Map[String, String]): Boolean =
    caseInsensitiveParams.getOrElse(FAIL_ON_DATA_LOSS_OPTION_KEY, "false").toBoolean

  private def pollTimeoutMs(caseInsensitiveParams: Map[String, String]): Int =
    caseInsensitiveParams
      .getOrElse(
        PulsarOptions.POLL_TIMEOUT_MS,
        (SparkEnv.get.conf.getTimeAsSeconds("spark.network.timeout", "120s") * 1000).toString)
      .toInt

  private def validateGeneralOptions(
      caseInsensitiveParams: Map[String, String]): Map[String, String] = {
    if (!caseInsensitiveParams.contains(SERVICE_URL_OPTION_KEY)) {
      throw new IllegalArgumentException(s"$SERVICE_URL_OPTION_KEY must be specified")
    }

    if (!caseInsensitiveParams.contains(ADMIN_URL_OPTION_KEY)) {
      throw new IllegalArgumentException(s"$ADMIN_URL_OPTION_KEY must be specified")
    }

    // validate topic options
    val topicOptions = caseInsensitiveParams.filter {
      case (k, _) => TOPIC_OPTION_KEYS.contains(k)
    }.toSeq
    if (topicOptions.isEmpty || topicOptions.size > 1) {
      throw new IllegalArgumentException(
        "You should specify topic(s) using one of the topic options: "
          + TOPIC_OPTION_KEYS.mkString(", "))
    }
    caseInsensitiveParams.find(x => TOPIC_OPTION_KEYS.contains(x._1)).get match {
      case ("topic", value) =>
        if (value.contains(",")) {
          throw new IllegalArgumentException(
            """Use "topics" instead of "topic" for multi topic read""")
        } else if (value.trim.isEmpty) {
          throw new IllegalArgumentException("No topic is specified")
        }

      case ("topics", value) =>
        val topics = value.split(",").map(_.trim).filter(_.nonEmpty)
        if (topics.isEmpty) {
          throw new IllegalArgumentException(
            s"No topics is specified for read with option: $value")
        }

      case ("topicspattern", value) =>
        if (value.trim.length == 0) {
          throw new IllegalArgumentException("TopicsPattern is empty")
        }
    }
    caseInsensitiveParams
  }

  private def validateStreamOptions(parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    caseInsensitiveParams
      .get(ENDING_OFFSETS_OPTION_KEY)
      .map(_ =>
        throw new IllegalArgumentException("ending offset not valid in streaming queries"))

    validateGeneralOptions(caseInsensitiveParams)
  }

  private def validateBatchOptions(parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    getPulsarOffset(caseInsensitiveParams, STARTING_OFFSETS_OPTION_KEY, EarliestOffset) match {
      case EarliestOffset => // good to go
      case LatestOffset =>
        throw new IllegalArgumentException(
          "starting offset can't be latest " +
            "for batch queries on Pulsar")
      case SpecificPulsarOffset(topicOffsets) =>
        topicOffsets.foreach {
          case (topic, offset) if offset == MessageId.latest =>
            throw new IllegalArgumentException(
              s"starting offset for $topic can't " +
                "be latest for batch queries on Pulsar")
          case _ => // ignore
        }
    }

    getPulsarOffset(caseInsensitiveParams, ENDING_OFFSETS_OPTION_KEY, LatestOffset) match {
      case EarliestOffset =>
        throw new IllegalArgumentException(
          "ending offset can't be earliest " +
            "for batch queries on Pulsar")
      case LatestOffset => // good to go
      case SpecificPulsarOffset(topicOffsets) =>
        topicOffsets.foreach {
          case (topic, offset) if offset == MessageId.earliest =>
            throw new IllegalArgumentException(
              s"ending offset for $topic can't " +
                "be earliest for batch queries on Pulsar")
          case _ => // ignore
        }
    }

    validateGeneralOptions(caseInsensitiveParams)
  }

  private def validateSinkOptions(parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }

    if (!caseInsensitiveParams.contains(SERVICE_URL_OPTION_KEY)) {
      throw new IllegalArgumentException(s"$SERVICE_URL_OPTION_KEY must be specified")
    }

    if (!caseInsensitiveParams.contains(ADMIN_URL_OPTION_KEY)) {
      throw new IllegalArgumentException(s"$ADMIN_URL_OPTION_KEY must be specified")
    }

    val topicOptions =
      caseInsensitiveParams.filter { case (k, _) => TOPIC_OPTION_KEYS.contains(k) }.toSeq.toMap
    if (topicOptions.size > 1 || topicOptions.contains(TOPIC_MULTI) || topicOptions.contains(
          TOPIC_PATTERN)) {
      throw new IllegalArgumentException(
        "Currently, we only support specify single topic through option, " +
          s"use '$TOPIC_SINGLE' to specify it.")
    }

    caseInsensitiveParams
  }

  private def prepareConfForConsumer(parameters: Map[String, String])
    : (ju.Map[String, Object], ju.Map[String, Object], String, String) = {

    val serviceUrl = getServiceUrl(parameters)
    val adminUrl = getAdminUrl(parameters)

    var clientParams = getClientParams(parameters)
    clientParams += (SERVICE_URL_OPTION_KEY -> serviceUrl)
    val consumerParams = getConsumerParams(parameters)

    (
      paramsToPulsarConf("pulsar.client", clientParams),
      paramsToPulsarConf("pulsar.consumer", consumerParams),
      serviceUrl,
      adminUrl
    )
  }

  private def prepareConfForProducer(parameters: Map[String, String])
    : (ju.Map[String, Object], ju.Map[String, Object], Option[String], String) = {

    val serviceUrl = getServiceUrl(parameters)
    val adminUrl = getAdminUrl(parameters)

    var clientParams = getClientParams(parameters)
    clientParams += (SERVICE_URL_OPTION_KEY -> serviceUrl)
    val producerParams = getProducerParams(parameters)

    val topic = parameters.get(TOPIC_SINGLE).map(_.trim).map(TopicName.get(_).toString)

    (
      paramsToPulsarConf("pulsar.client", clientParams),
      paramsToPulsarConf("pulsar.producer", producerParams),
      topic,
      adminUrl
    )
  }

  private def jsonOptions: JSONOptionsInRead = {
    val spark = SparkSession.getActiveSession.get
    new JSONOptionsInRead(
      CaseInsensitiveMap(Map.empty),
      spark.sessionState.conf.sessionLocalTimeZone,
      spark.sessionState.conf.columnNameOfCorruptRecord)
  }
}
