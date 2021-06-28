package org.apache.spark.sql.pulsar

import java.{util => ju}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import scala.collection.JavaConverters._

private[pulsar] class PulsarTable(structType: StructType)
  extends Table
    with SupportsRead
    with SupportsWrite with Logging {

  import PulsarProvider._

  override def name(): String = "PulsarTable"

  override def schema(): StructType = {
    logDebug(s"Pulsar table structType: $structType")
    structType
  }

  override def capabilities(): ju.Set[TableCapability] = {
    import TableCapability._
    Set(BATCH_READ, MICRO_BATCH_READ, BATCH_WRITE, STREAMING_WRITE, CONTINUOUS_READ, ACCEPT_ANY_SCHEMA).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder = () => new PulsarScan(structType, options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val options = info.options()
    val (clientConf, producerConf, topic, adminUrl) = prepareConfForProducer(options.asScala.toMap)
    PulsarSinks.validateQuery(schema().toAttributes, topic)

    new PulsarWriter(schema(), clientConf, producerConf, topic, adminUrl)
  }
}


