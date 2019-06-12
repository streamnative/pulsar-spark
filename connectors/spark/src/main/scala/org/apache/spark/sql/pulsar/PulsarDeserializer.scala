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

import java.nio.ByteBuffer
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.util.Date

import scala.collection.JavaConverters._

import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.Schema
import org.apache.avro.Schema.Type._
import org.apache.avro.generic.GenericFixed
import org.apache.avro.util.Utf8
import org.apache.pulsar.client.api.Message
import org.apache.pulsar.client.api.schema.{GenericRecord => PGenericRecord}
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecificInternalRow
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.util.{DateTimeUtils}
import org.apache.spark.unsafe.types.UTF8String

import scala.collection.mutable.ArrayBuffer

class PulsarDeserializer(schemaInfo: SchemaInfo) {

  val rootDataType: DataType = SchemaUtils.si2SqlType(schemaInfo)

  import SchemaUtils._

  def deserialize(message: Message[_]): SpecificInternalRow = converter(message)

  private val converter: Message[_] => SpecificInternalRow = {

    schemaInfo.getType match {
      case SchemaType.AVRO =>
        val st = rootDataType.asInstanceOf[StructType]
        val resultRow = new SpecificInternalRow(st.map(_.dataType) ++ metaDataFields.map(_.dataType))
        val fieldUpdater = new RowUpdater(resultRow)
        val avroSchema = new org.apache.avro.Schema.Parser().parse(
          new String(schemaInfo.getSchema, StandardCharsets.UTF_8))
        val writer = getRecordWriter(avroSchema, st, Nil)
        (msg: Message[_]) => {
          val value = msg.getValue
          writer(fieldUpdater, value.asInstanceOf[PGenericRecord])
          writeMetadataFields(msg, resultRow)
          resultRow
        }
      case _ => // AtomicTypes
        val tmpRow = new SpecificInternalRow(Seq(rootDataType) ++ metaDataFields.map(_.dataType))
        val fieldUpdater = new RowUpdater(tmpRow)
        val writer = newAtomicWriter(rootDataType)
        (msg: Message[_]) => {
          val value = msg.getValue
          writer(fieldUpdater, 0, value)
          writeMetadataFields(msg, tmpRow)
          tmpRow
        }
    }
  }

  def writeMetadataFields(message: Message[_], row: SpecificInternalRow) = {
    val metaStartIdx = row.numFields - 5
    // key
    if (message.hasKey) {
      row.update(metaStartIdx, message.getKeyBytes)
    } else {
      row.setNullAt(metaStartIdx)
    }
    // topic
    row.update(metaStartIdx + 1, UTF8String.fromString(message.getTopicName))
    // messageId
    row.update(metaStartIdx + 2, message.getMessageId.toByteArray)
    // publish time
    row.setLong(
      metaStartIdx + 3,
      DateTimeUtils.fromJavaTimestamp(new Timestamp(message.getPublishTime)))
    // event time
    if (message.getEventTime > 0) {
      row.setLong(
        metaStartIdx + 4,
        DateTimeUtils.fromJavaTimestamp(new Timestamp(message.getEventTime)))
    } else {
      row.setNullAt(metaStartIdx + 4)
    }
  }

  private def newAtomicWriter(dataType: DataType):
      (RowUpdater, Int, Any) => Unit =
    dataType match {
      case ByteType => (updater, ordinal, value) =>
        updater.setByte(ordinal, value.asInstanceOf[Byte])

      case BooleanType => (updater, ordinal, value) =>
        updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case IntegerType => (updater, ordinal, value) =>
        updater.setInt(ordinal, value.asInstanceOf[Int])

      case DateType => (updater, ordinal, value) =>
        updater.setInt(ordinal,
          (value.asInstanceOf[Date].getTime / DateTimeUtils.MILLIS_PER_DAY).toInt)

      case LongType => (updater, ordinal, value) =>
        updater.setLong(ordinal, value.asInstanceOf[Long])

      case TimestampType => (updater, ordinal, value) =>
        updater.setLong(ordinal,
          DateTimeUtils.fromJavaTimestamp(value.asInstanceOf[Timestamp]))

      case FloatType => (updater, ordinal, value) =>
        updater.setFloat(ordinal, value.asInstanceOf[Float])

      case DoubleType => (updater, ordinal, value) =>
        updater.setDouble(ordinal, value.asInstanceOf[Double])

      case ShortType => (updater, ordinal, value) =>
        updater.setShort(ordinal, value.asInstanceOf[Short])

      case StringType => (updater, ordinal, value) =>
        val str = UTF8String.fromString(value.asInstanceOf[String])
        updater.set(ordinal, str)

      case BinaryType => (updater, ordinal, value) =>
        val bytes = value match {
          case b: ByteBuffer =>
            val bytes = new Array[Byte](b.remaining)
            b.get(bytes)
            bytes
          case b: Array[Byte] => b
          case other => throw new RuntimeException(s"$other is not a valid avro binary.")
        }
        updater.set(ordinal, bytes)

      case tpe =>
        throw new NotImplementedError(s"$tpe is not supported for the moment")
    }

  private def newWriter(
      avroType: Schema,
      catalystType: DataType,
      path: List[String]): (RowUpdater, Int, Any) => Unit =
    (avroType.getType, catalystType) match {
      case (NULL, NullType) => (updater, ordinal, _) =>
        updater.setNullAt(ordinal)

      case (BOOLEAN, BooleanType) => (updater, ordinal, value) =>
        updater.setBoolean(ordinal, value.asInstanceOf[Boolean])

      case (INT, IntegerType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, value.asInstanceOf[Int])

      case (INT, DateType) => (updater, ordinal, value) =>
        updater.setInt(ordinal, value.asInstanceOf[Int])

      case (LONG, LongType) => (updater, ordinal, value) =>
        updater.setLong(ordinal, value.asInstanceOf[Long])

      case (LONG, TimestampType) => avroType.getLogicalType match {
        case _: TimestampMillis => (updater, ordinal, value) =>
          updater.setLong(ordinal, value.asInstanceOf[Long] * 1000)
        case _: TimestampMicros => (updater, ordinal, value) =>
          updater.setLong(ordinal, value.asInstanceOf[Long])
        case other => throw new IncompatibleSchemaException(
          s"Cannot convert Avro logical type ${other} to Catalyst Timestamp type.")
      }

      case (FLOAT, FloatType) => (updater, ordinal, value) =>
        updater.setFloat(ordinal, value.asInstanceOf[Float])

      case (DOUBLE, DoubleType) => (updater, ordinal, value) =>
        updater.setDouble(ordinal, value.asInstanceOf[Double])

      case (STRING, StringType) => (updater, ordinal, value) =>
        val str = value match {
          case s: String => UTF8String.fromString(s)
          case s: Utf8 =>
            val bytes = new Array[Byte](s.getByteLength)
            System.arraycopy(s.getBytes, 0, bytes, 0, s.getByteLength)
            UTF8String.fromBytes(bytes)
        }
        updater.set(ordinal, str)

      case (ENUM, StringType) => (updater, ordinal, value) =>
        updater.set(ordinal, UTF8String.fromString(value.toString))

      case (FIXED, BinaryType) => (updater, ordinal, value) =>
        updater.set(ordinal, value.asInstanceOf[GenericFixed].bytes().clone())

      case (BYTES, BinaryType) => (updater, ordinal, value) =>
        val bytes = value match {
          case b: ByteBuffer =>
            val bytes = new Array[Byte](b.remaining)
            b.get(bytes)
            bytes
          case b: Array[Byte] => b
          case other => throw new RuntimeException(s"$other is not a valid avro binary.")
        }
        updater.set(ordinal, bytes)

      case (FIXED, d: DecimalType) =>
        throw new NotImplementedError(s"$d not supported for now")

      case (BYTES, d: DecimalType) =>
        throw new NotImplementedError(s"$d not supported for now")

      case (RECORD, st: StructType) =>
        val writeRecord = getRecordWriter(avroType, st, path)
        (updater, ordinal, value) =>
          val row = new SpecificInternalRow(st)
          writeRecord(new RowUpdater(row), value.asInstanceOf[PGenericRecord])
          updater.set(ordinal, row)

      case (ARRAY, ArrayType(elementType, containsNull)) =>
        throw new NotImplementedError("arrayType not supported for now")

      case (MAP, MapType(keyType, valueType, valueContainsNull)) if keyType == StringType =>
        throw new NotImplementedError("mapType not supported for now")

      // avro uses Union type to represent a nullable field
      case (UNION, _) =>
        val allTypes = avroType.getTypes.asScala
        val nonNullTypes = allTypes.filter(_.getType != NULL)
        if (nonNullTypes.nonEmpty) {
          if (nonNullTypes.length == 1) {
            newWriter(nonNullTypes.head, catalystType, path)
          } else {
            throw new NotImplementedError("UnionType not supported for now")
          }
        } else {
          (updater, ordinal, value) => updater.setNullAt(ordinal)
        }

      case _ =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Avro to catalyst because schema at path ${path.mkString(".")} " +
            s"is not compatible (avroType = $avroType, sqlType = $catalystType).\n" +
            s"Source Avro schema: $avroType.\n" +
            s"Target Catalyst type: $catalystType")
    }

  private def getRecordWriter(
      avroType: Schema,
      sqlType: StructType,
      path: List[String]): (RowUpdater, PGenericRecord) => Unit = {
    val validFieldIndexes = ArrayBuffer.empty[Int]
    val validFieldNames = ArrayBuffer.empty[String]
    val fieldWriters = ArrayBuffer.empty[(RowUpdater, Any) => Unit]

    val length = sqlType.length
    var i = 0
    while (i < length) {
      val sqlField = sqlType.fields(i)
      val avroField = avroType.getField(sqlField.name)
      if (avroField != null) {
        validFieldIndexes += avroField.pos()
        validFieldNames += sqlField.name

        val baseWriter = newWriter(avroField.schema(), sqlField.dataType, path :+ sqlField.name)
        val ordinal = i
        val fieldWriter = (fieldUpdater: RowUpdater, value: Any) => {
          if (value == null) {
            fieldUpdater.setNullAt(ordinal)
          } else {
            baseWriter(fieldUpdater, ordinal, value)
          }
        }
        fieldWriters += fieldWriter
      } else if (!sqlField.nullable) {
        throw new IncompatibleSchemaException(
          s"""
             |Cannot find non-nullable field ${path.mkString(".")}.${sqlField.name} in Avro schema.
             |Source Avro schema: $avroType.
             |Target Catalyst type: $sqlType.
           """.stripMargin)
      }
      i += 1
    }

    (fieldUpdater, record) => {
      var i = 0
      while (i < validFieldIndexes.length) {
        fieldWriters(i)(fieldUpdater, record.getField(validFieldNames(i)))
        i += 1
      }
    }
  }
}

class RowUpdater(row: InternalRow) {
  def set(ordinal: Int, value: Any): Unit = row.update(ordinal, value)

  def setNullAt(ordinal: Int): Unit = row.setNullAt(ordinal)
  def setBoolean(ordinal: Int, value: Boolean): Unit = row.setBoolean(ordinal, value)
  def setByte(ordinal: Int, value: Byte): Unit = row.setByte(ordinal, value)
  def setShort(ordinal: Int, value: Short): Unit = row.setShort(ordinal, value)
  def setInt(ordinal: Int, value: Int): Unit = row.setInt(ordinal, value)
  def setLong(ordinal: Int, value: Long): Unit = row.setLong(ordinal, value)
  def setDouble(ordinal: Int, value: Double): Unit = row.setDouble(ordinal, value)
  def setFloat(ordinal: Int, value: Float): Unit = row.setFloat(ordinal, value)
}
