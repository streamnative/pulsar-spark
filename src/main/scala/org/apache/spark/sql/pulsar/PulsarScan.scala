package org.apache.spark.sql.pulsar

import java.util.UUID

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.read.{Batch, Scan}
import org.apache.spark.sql.connector.read.streaming.{ContinuousStream, MicroBatchStream}
import org.apache.spark.sql.pulsar.PulsarSourceUtils.reportDataLossFunc
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

import scala.collection.JavaConverters._

private[pulsar] class PulsarScan(structType: StructType, options: CaseInsensitiveStringMap)
  extends Scan
  with Logging {

  import PulsarOptions._
  import PulsarProvider._

  override def readSchema(): StructType = structType

  override def toBatch: Batch = {
    val parameters = options.asScala.toMap
    val subscriptionNamePrefix = s"${parameters.getOrElse(SUBSCRIPTION_PREFIX_OPTION_KEY, s"spark-pulsar-batch-${UUID.randomUUID}")}"

    val caseInsensitiveParams = validateBatchOptions(parameters)
    val (clientConf, readerConf, serviceUrl, adminUrl) = prepareConfForReader(parameters)
    val tpVersionOpt = parameters.get(PulsarOptions.TOPIC_VERSION)

    val (start, end, structSchema, pSchema) = Utils.tryWithResource(
      PulsarMetadataReader(
        serviceUrl,
        adminUrl,
        clientConf,
        subscriptionNamePrefix,
        caseInsensitiveParams)) { reader =>
      val perTopicStarts = reader.startingOffsetForEachTopic(
        caseInsensitiveParams,
        EarliestOffset)
      val startingOffset = SpecificPulsarOffset(
        reader.actualOffsets(
          perTopicStarts,
          pollTimeoutMs(caseInsensitiveParams),
          reportDataLossFunc(failOnDataLoss(caseInsensitiveParams))))

      val endingOffset =
        reader.offsetForEachTopic(
          caseInsensitiveParams, ENDING_OFFSETS_OPTION_KEY, LatestOffset)

      val pulsarSchema = reader.getPulsarSchema(tpVersionOpt)
      val structSchema = SchemaUtils.pulsarSourceSchema(pulsarSchema)
      (startingOffset, endingOffset, structSchema, pulsarSchema)
    }
    new PulsarBatch(
      structSchema,
      new SchemaInfoSerializable(pSchema),
      clientConf,
      readerConf,
      start,
      end,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions(caseInsensitiveParams)
    )
  }

  override def toMicroBatchStream(checkpointLocation: String): MicroBatchStream = {
    val parameters = options.asScala.toMap
    val subscriptionNamePrefix = s"${parameters.getOrElse(SUBSCRIPTION_PREFIX_OPTION_KEY, s"spark-pulsar-${UUID.randomUUID}-${checkpointLocation.hashCode}")}"

    val caseInsensitiveParams = validateStreamOptions(parameters)
    val (clientConf, readerConf, serviceUrl, adminUrl) = prepareConfForReader(parameters)

    val metadataReader = PulsarMetadataReader(
      serviceUrl,
      adminUrl,
      clientConf,
      subscriptionNamePrefix,
      caseInsensitiveParams)

    // start from latest offset if not specified to be consistent with Pulsar source
    val offset = metadataReader.startingOffsetForEachTopic(
      caseInsensitiveParams,
      LatestOffset)

    try {
      metadataReader.setupCursor(offset)
    } catch {
      case e: Exception => logWarning(s"Exception while setup cursor ${e.getMessage}")
    }

    new PulsarMicroBatchStream(
      metadataReader,
      clientConf,
      readerConf,
      checkpointLocation,
      offset,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions(caseInsensitiveParams)
    )
  }

  override def toContinuousStream(checkpointLocation: String): ContinuousStream = {
    val parameters = options.asScala.toMap
    val caseInsensitiveParams = validateStreamOptions(parameters)
    val (clientConf, readerConf, serviceUrl, adminUrl) = prepareConfForReader(parameters)
    val subscriptionNamePrefix = s"${parameters.getOrElse(SUBSCRIPTION_PREFIX_OPTION_KEY, s"spark-pulsar-${UUID.randomUUID}-${checkpointLocation.hashCode}")}"

    val metadataReader = PulsarMetadataReader(
      serviceUrl,
      adminUrl,
      clientConf,
      subscriptionNamePrefix,
      caseInsensitiveParams)

    //metadataReader.getAndCheckCompatible(schema)

    val offset = metadataReader.startingOffsetForEachTopic(
      caseInsensitiveParams,
      LatestOffset)

    try {
      metadataReader.setupCursor(offset)
    } catch {
      case e: Exception => logWarning(s"Exception while setup cursor ${e.getMessage}")
    }

    new PulsarContinuousReader(
      metadataReader,
      clientConf,
      readerConf,
      offset,
      pollTimeoutMs(caseInsensitiveParams),
      failOnDataLoss(caseInsensitiveParams),
      subscriptionNamePrefix,
      jsonOptions(caseInsensitiveParams))
  }
}

