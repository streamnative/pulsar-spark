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

import java.{util => ju}
import java.util.{Locale, UUID}

import org.apache.pulsar.client.api.MessageId
import org.apache.pulsar.common.naming.TopicName

import org.apache.spark.SparkEnv
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{AnalysisException, DataFrame, SaveMode, SparkSession, SQLContext}
import org.apache.spark.sql.catalyst.json.JSONOptionsInRead
import org.apache.spark.sql.catalyst.util.CaseInsensitiveMap
import org.apache.spark.sql.execution.streaming.{Sink, Source}
import org.apache.spark.sql.pulsar.PulsarSourceUtils.reportDataLossFunc
import org.apache.spark.sql.sources._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * The provider class for all Pulsar readers and writers. It is designed such that it throws
 * IllegalArgumentException when the Pulsar Dataset is created, so that it can catch missing
 * options even before the query is started.
 */
private[pulsar] class PulsarProvider
    extends DataSourceRegister
    with StreamSourceProvider
    with StreamSinkProvider
    with RelationProvider
    with CreatableRelationProvider
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
    val (clientConfig, _, adminClientConfig, serviceUrlConfig, adminUrlConfig) =
      prepareConfForReader(parameters)

    val subscriptionNamePrefix = s"spark-pulsar-${UUID.randomUUID}"
    val inferredSchema = Utils.tryWithResource(
      new PulsarMetadataReader(
        serviceUrlConfig,
        adminUrlConfig,
        clientConfig,
        adminClientConfig,
        subscriptionNamePrefix,
        caseInsensitiveParams,
        getAllowDifferentTopicSchemas(parameters),
        getPredefinedSubscription(parameters))) { reader =>
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
    val (clientConfig, readerConfig, adminClientConfig, serviceUrl, adminUrl) =
      prepareConfForReader(parameters)

    val subscriptionNamePrefix = getSubscriptionPrefix(parameters)
    val metadataReader = new PulsarMetadataReader(
      serviceUrl,
      adminUrl,
      clientConfig,
      adminClientConfig,
      subscriptionNamePrefix,
      caseInsensitiveParams,
      getAllowDifferentTopicSchemas(parameters),
      getPredefinedSubscription(parameters))

    metadataReader.getAndCheckCompatible(schema)

    // start from latest offset if not specified to be consistent with Pulsar source
    val offset =
      metadataReader.offsetForEachTopic(caseInsensitiveParams, LatestOffset, StartOptionKey)
    metadataReader.setupCursor(offset)

    new PulsarSource(
      sqlContext,
      metadataReader,
      clientConfig,
      readerConfig,
      metadataPath,
      offset,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions,
      maxEntriesPerTrigger(caseInsensitiveParams)
    )
  }

  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val caseInsensitiveParams = validateBatchOptions(parameters)

    val subscriptionNamePrefix = getSubscriptionPrefix(parameters, isBatch = true)

    val (clientConfig, readerConfig, adminClientConfig, serviceUrl, adminUrl) =
      prepareConfForReader(parameters)
    val (start, end, schema, pSchema) = Utils.tryWithResource(
      new PulsarMetadataReader(
        serviceUrl,
        adminUrl,
        clientConfig,
        adminClientConfig,
        subscriptionNamePrefix,
        caseInsensitiveParams,
        getAllowDifferentTopicSchemas(parameters),
        getPredefinedSubscription(parameters))) { reader =>
      val perTopicStarts =
        reader.offsetForEachTopic(caseInsensitiveParams, EarliestOffset, StartOptionKey)
      val startingOffset = SpecificPulsarOffset(
        reader.actualOffsets(
          perTopicStarts,
          pollTimeoutMs(caseInsensitiveParams),
          reportDataLossFunc(failOnDataLoss(caseInsensitiveParams))))
      val perTopicEnds =
        reader.offsetForEachTopic(caseInsensitiveParams, LatestOffset, EndOptionKey)
      val endingOffset = SpecificPulsarOffset(
        reader.actualOffsets(
          perTopicEnds,
          pollTimeoutMs(caseInsensitiveParams),
          reportDataLossFunc(failOnDataLoss(caseInsensitiveParams))))

      val pulsarSchema = reader.getPulsarSchema()
      val schema = SchemaUtils.pulsarSourceSchema(pulsarSchema)
      (startingOffset, endingOffset, schema, pulsarSchema)
    }

    new PulsarRelation(
      sqlContext,
      schema,
      new SchemaInfoSerializable(pSchema),
      adminUrl,
      clientConfig,
      readerConfig,
      start,
      end,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions)
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

    val (clientConfig, producerConfig, topic, adminUrl) = prepareConfForProducer(parameters)
    PulsarSinks.validateQuery(data.schema.toAttributes, topic)

    PulsarSinks.write(
      sqlContext.sparkSession,
      data.queryExecution,
      clientConfig,
      producerConfig,
      topic,
      adminUrl)

    /**
     * This method is suppose to return a relation the data that was written. Currently we haven't
     * supported schema yet. Therefore, we return an empty base relation for now.
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

    val (clientConfig, producerConfig, topic, adminUrl) = prepareConfForProducer(parameters)

    new PulsarSink(sqlContext, clientConfig, producerConfig, topic, adminUrl)
  }
}

private[pulsar] object PulsarProvider extends Logging {
  import PulsarConfigurationUtils._
  import PulsarOptions._

  val LATEST_TIME = -2L
  val EARLIEST_TIME = -1L

  private def getClientParams(parameters: Map[String, String]): Map[String, String] = {
    val lowercaseKeyMap = parameters.keySet
      .filter(_.startsWith(PulsarClientOptionKeyPrefix))
      .map { k =>
        k.drop(PulsarClientOptionKeyPrefix.length).toString -> parameters(k)
      }
      .toMap
    lowercaseKeyMap.map { case (k, v) =>
      clientConfKeys.getOrElse(
        k,
        throw new IllegalArgumentException(s"$k not supported by pulsar")) -> v
    }
  }

  private def getProducerParams(parameters: Map[String, String]): Map[String, String] = {
    getModuleParams(parameters, PulsarProducerOptionKeyPrefix, producerConfKeys)
  }

  private def getReaderParams(parameters: Map[String, String]): Map[String, String] = {
    getModuleParams(parameters, PulsarReaderOptionKeyPrefix, readerConfKeys)
  }

  private def getAdminParams(parameters: Map[String, String]): Map[String, String] = {
    getModuleParams(parameters, PulsarAdminOptionKeyPrefix, clientConfKeys)
  }

  private def getModuleParams(
      connectorConfiguration: Map[String, String],
      modulePrefix: String,
      moduleKeyLookup: Map[String, String]): Map[String, String] = {
    val lowerCaseModuleParameters = connectorConfiguration.keySet
      .filter(_.startsWith(modulePrefix))
      .map { k =>
        k.drop(modulePrefix.length) -> connectorConfiguration(k)
      }
      .toMap
    lowerCaseModuleParameters.map { case (k, v) =>
      moduleKeyLookup.getOrElse(
        k,
        throw new IllegalArgumentException(s"$k not supported by pulsar")) -> v
    }
  }

  private def hasAdminParams(parameters: Map[String, String]): Boolean = {
    getAdminParams(parameters).isEmpty == false
  }

  def getPulsarOffset(
      params: Map[String, String],
      defaultOffsets: PulsarOffset,
      optionKey: String): PulsarOffset = {

    val offsets = optionKey match {
      case StartOptionKey =>
        params.get(StartingOffsetsOptionKey).map(_.trim)
      case EndOptionKey =>
        params.get(EndingOffsetsOptionKey).map(_.trim)
    }
    val time = optionKey match {
      case StartOptionKey =>
        params.get(StartingTime).map(_.trim)
      case EndOptionKey =>
        params.get(EndingTime).map(_.trim)
    }

    if (offsets.isDefined && time.isDefined) {
      throw new IllegalArgumentException(
        s"You can only specify starting $optionKey through " +
          s"either $StartingOffsetsOptionKey or $StartingTime, not both.")
    }

    val result = if (offsets.isDefined) {
      offsets match {
        case Some(offset) if offset.toLowerCase(Locale.ROOT) == "latest" =>
          LatestOffset
        case Some(offset) if offset.toLowerCase(Locale.ROOT) == "earliest" =>
          EarliestOffset
        case Some(json) =>
          SpecificPulsarOffset(JsonUtils.topicOffsets(json))
        case None => defaultOffsets
      }
    } else if (time.isDefined) {
      time match {
        case Some(json) if json.startsWith("{") =>
          SpecificPulsarTime(JsonUtils.topicTimes(json))
        case Some(t) => // try to convert it as long if it's not a map
          try {
            TimeOffset(t.toLong)
          } catch {
            case e: NumberFormatException =>
              throw new IllegalArgumentException(
                s"$optionKey time $t cannot be converted to Long")
          }
        case None => defaultOffsets
      }
    } else {
      defaultOffsets
    }

    result
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

  private def getSubscriptionPrefix(
      parameters: Map[String, String],
      isBatch: Boolean = false): String = {
    val defaultPrefix = if (isBatch) "spark-pulsar-batch" else "spark-pulsar"
    parameters.getOrElse(SubscriptionPrefix, s"$defaultPrefix-${UUID.randomUUID}")
  }

  private def getPredefinedSubscription(parameters: Map[String, String]): Option[String] = {
    val sub = parameters.getOrElse(PredefinedSubscription, "")
    sub match {
      case "" => None
      case s => Option(s)
    }
  }

  private def getServiceUrl(parameters: Map[String, String]): String = {
    parameters(ServiceUrlOptionKey)
  }

  private def getAdminUrl(parameters: Map[String, String]): String = {
    parameters(AdminUrlOptionKey)
  }

  private def getAllowDifferentTopicSchemas(parameters: Map[String, String]): Boolean = {
    parameters.getOrElse(AllowDifferentTopicSchemas, "false").toBoolean
  }

  private def failOnDataLoss(caseInsensitiveParams: Map[String, String]): Boolean =
    caseInsensitiveParams.getOrElse(FailOnDataLossOptionKey, "false").toBoolean

  private def pollTimeoutMs(caseInsensitiveParams: Map[String, String]): Int =
    caseInsensitiveParams
      .getOrElse(
        PulsarOptions.PollTimeoutMS,
        (SparkEnv.get.conf.getTimeAsSeconds("spark.network.timeout", "120s") * 1000).toString)
      .toInt

  private def maxEntriesPerTrigger(caseInsensitiveParams: Map[String, String]): Long =
    caseInsensitiveParams.getOrElse(MaxEntriesPerTrigger, "-1").toLong

  private def validateGeneralOptions(
      caseInsensitiveParams: Map[String, String]): Map[String, String] = {
    if (!caseInsensitiveParams.contains(ServiceUrlOptionKey)) {
      throw new IllegalArgumentException(s"$ServiceUrlOptionKey must be specified")
    }

    if (!caseInsensitiveParams.contains(AdminUrlOptionKey)) {
      throw new IllegalArgumentException(s"$AdminUrlOptionKey must be specified")
    }

    // validate topic options
    val topicOptions = caseInsensitiveParams.filter { case (k, _) =>
      TopicOptionKeys.contains(k)
    }.toSeq
    if (topicOptions.isEmpty || topicOptions.size > 1) {
      throw new IllegalArgumentException(
        "You should specify topic(s) using one of the topic options: "
          + TopicOptionKeys.mkString(", "))
    }
    topicOptions.head match {
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
        if (value.trim.isEmpty) {
          throw new IllegalArgumentException("TopicsPattern is empty")
        }
    }
    caseInsensitiveParams
  }

  private def validateStreamOptions(parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    caseInsensitiveParams
      .get(EndingOffsetsOptionKey)
      .map(_ =>
        throw new IllegalArgumentException("ending offset not valid in streaming queries"))

    validateGeneralOptions(caseInsensitiveParams)
  }

  private def validateBatchOptions(parameters: Map[String, String]): Map[String, String] = {
    val caseInsensitiveParams = parameters.map { case (k, v) => (k.toLowerCase(Locale.ROOT), v) }
    getPulsarOffset(caseInsensitiveParams, StartingOffsetsOptionKey, EarliestOffset) match {
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

    getPulsarOffset(caseInsensitiveParams, EndingOffsetsOptionKey, LatestOffset) match {
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

    if (!caseInsensitiveParams.contains(ServiceUrlOptionKey)) {
      throw new IllegalArgumentException(s"$ServiceUrlOptionKey must be specified")
    }

    if (!caseInsensitiveParams.contains(AdminUrlOptionKey)) {
      throw new IllegalArgumentException(s"$AdminUrlOptionKey must be specified")
    }

    val topicOptions =
      caseInsensitiveParams.filter { case (k, _) => TopicOptionKeys.contains(k) }.toSeq.toMap
    if (topicOptions.size > 1 || topicOptions.contains(TopicMulti) || topicOptions.contains(
        TopicPattern)) {
      throw new IllegalArgumentException(
        "Currently, we only support specify single topic through option, " +
          s"use '$TopicSingle' to specify it.")
    }

    caseInsensitiveParams
  }

  private def prepareConfForReader(parameters: Map[String, String]): (
      ju.Map[String, Object],
      ju.Map[String, Object],
      ju.Map[String, Object],
      String,
      String) = {

    val serviceUrl = getServiceUrl(parameters)
    val adminUrl = getAdminUrl(parameters)

    var clientParams = getClientParams(parameters)
    clientParams += (ServiceUrlOptionKey -> serviceUrl)
    val readerParams = getReaderParams(parameters)
    val adminParams = Option(getAdminParams(parameters))
      .filter(_.nonEmpty)
      .getOrElse(clientParams)

    (
      paramsToPulsarConf("pulsar.client", clientParams),
      paramsToPulsarConf("pulsar.reader", readerParams),
      paramsToPulsarConf("pulsar.admin", adminParams),
      serviceUrl,
      adminUrl)
  }

  private def prepareConfForProducer(parameters: Map[String, String])
      : (ju.Map[String, Object], ju.Map[String, Object], Option[String], String) = {

    val serviceUrl = getServiceUrl(parameters)
    val adminUrl = getAdminUrl(parameters)

    var clientParams = getClientParams(parameters)
    clientParams += (ServiceUrlOptionKey -> serviceUrl)
    val producerParams = getProducerParams(parameters)

    val topic = parameters.get(TopicSingle).map(_.trim).map(TopicName.get(_).toString)

    (
      paramsToPulsarConf("pulsar.client", clientParams),
      paramsToPulsarConf("pulsar.producer", producerParams),
      topic,
      adminUrl)
  }

  private def jsonOptions: JSONOptionsInRead = {
    val spark = SparkSession.getActiveSession.get
    new JSONOptionsInRead(
      CaseInsensitiveMap(Map.empty),
      spark.sessionState.conf.sessionLocalTimeZone,
      spark.sessionState.conf.columnNameOfCorruptRecord)
  }
}
