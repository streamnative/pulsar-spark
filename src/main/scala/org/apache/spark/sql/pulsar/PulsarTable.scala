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

import scala.collection.JavaConverters._

import PulsarProvider._

import org.apache.spark.internal.Logging
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.catalog.TableCapability._
import org.apache.spark.sql.connector.read.ScanBuilder
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, WriteBuilder}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap


private[pulsar] class PulsarTable(structType: StructType)
  extends Table
    with SupportsRead
    with SupportsWrite with Logging {


  override def name(): String = "PulsarTable"

  override def schema(): StructType = {
    logDebug(s"Pulsar table structType: $structType")
    structType
  }

  override def capabilities(): ju.Set[TableCapability] = {
    Set(BATCH_READ, MICRO_BATCH_READ,
      BATCH_WRITE, STREAMING_WRITE,
      CONTINUOUS_READ,
      ACCEPT_ANY_SCHEMA).asJava
  }

  override def newScanBuilder(options: CaseInsensitiveStringMap): ScanBuilder =
    () => new PulsarScan(structType, options)

  override def newWriteBuilder(info: LogicalWriteInfo): WriteBuilder = {
    val options = info.options()
    val (clientConf, producerConf, topic, adminUrl) = prepareConfForProducer(options.asScala.toMap)
    PulsarSinks.validateQuery(schema().toAttributes, topic)

    new PulsarWriter(schema(), clientConf, producerConf, topic, adminUrl)
  }
}


