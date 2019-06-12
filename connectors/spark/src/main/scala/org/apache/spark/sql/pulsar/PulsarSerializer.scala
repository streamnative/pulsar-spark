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

import scala.collection.JavaConverters._

import org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.avro.{Schema => ASchema}
import org.apache.avro.Schema.Type
import org.apache.avro.Schema.Type._
import org.apache.avro.util.Utf8
import org.apache.pulsar.client.api.schema.GenericRecord

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

class PulsarSerializer(
    rootCatalystType: DataType,
    nullable: Boolean) {

  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private val rootAvroType = SchemaUtils.sqlType2ASchema(rootCatalystType)

  private val converter: Any => Any = {
    val actualAvroType = resolveNullableType(rootAvroType, nullable)
    val baseConverter = rootCatalystType match {
      case st: StructType =>
        newStructConverter(st, actualAvroType).asInstanceOf[Any => Any]
      case _ =>
        val converter = singleValueConverter(rootCatalystType, actualAvroType)
        (data: Any) =>
          converter.apply(data.asInstanceOf[SpecializedGetters], 0)
    }
    if (nullable) {
      (data: Any) =>
        if (data == null) {
          null
        } else {
          baseConverter.apply(data)
        }
    } else {
      baseConverter
    }
  }

  private def singleValueConverter(catalystType: DataType, avroType: ASchema): Converter = {
    (catalystType, avroType.getType) match {
      case (NullType, NULL) =>
        (getter, ordinal) => null
      case (BooleanType, BOOLEAN) =>
        (getter, ordinal) => getter.getBoolean(ordinal)
      case (ByteType, INT) =>
        (getter, ordinal) => getter.getByte(ordinal)
      case (ShortType, INT) =>
        (getter, ordinal) => getter.getShort(ordinal)
      case (IntegerType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)
      case (LongType, LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)
      case (FloatType, FLOAT) =>
        (getter, ordinal) => getter.getFloat(ordinal)
      case (DoubleType, DOUBLE) =>
        (getter, ordinal) => getter.getDouble(ordinal)
      case (StringType, STRING) =>
        (getter, ordinal) => getter.getUTF8String(ordinal).toString
      case (BinaryType, BYTES) =>
        (getter, ordinal) => getter.getBinary(ordinal)
      case (DateType, INT) =>
        (getter, ordinal) => DateTimeUtils.toJavaDate(getter.getInt(ordinal))

      case (TimestampType, LONG) => avroType.getLogicalType match {
        case _: TimestampMillis => (getter, ordinal) =>
          DateTimeUtils.toJavaTimestamp(getter.getLong(ordinal) / 1000)
        case _: TimestampMicros => (getter, ordinal) =>
          DateTimeUtils.toJavaTimestamp(getter.getLong(ordinal))
        case other => throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
      }

      case other =>
        throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystType to " +
          s"Avro type $avroType.")
    }
  }

  private type Converter = (SpecializedGetters, Int) => Any

  private def newConverter(catalystType: DataType, avroType: ASchema): Converter = {
    (catalystType, avroType.getType) match {
      case (NullType, NULL) =>
        (getter, ordinal) => null
      case (BooleanType, BOOLEAN) =>
        (getter, ordinal) => getter.getBoolean(ordinal)
      case (ByteType, INT) =>
        (getter, ordinal) => getter.getByte(ordinal).toInt
      case (ShortType, INT) =>
        (getter, ordinal) => getter.getShort(ordinal).toInt
      case (IntegerType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)
      case (LongType, LONG) =>
        (getter, ordinal) => getter.getLong(ordinal)
      case (FloatType, FLOAT) =>
        (getter, ordinal) => getter.getFloat(ordinal)
      case (DoubleType, DOUBLE) =>
        (getter, ordinal) => getter.getDouble(ordinal)
      case (StringType, STRING) =>
        (getter, ordinal) =>
          val ov = ordinal
          new Utf8(getter.getUTF8String(ordinal).getBytes)
      case (BinaryType, BYTES) =>
        (getter, ordinal) => ByteBuffer.wrap(getter.getBinary(ordinal))
      case (DateType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)

      case (TimestampType, LONG) => avroType.getLogicalType match {
        case _: TimestampMillis => (getter, ordinal) => getter.getLong(ordinal) / 1000
        case _: TimestampMicros => (getter, ordinal) => getter.getLong(ordinal)
        case other => throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
      }

      case (st: StructType, RECORD) =>
        val structConverter = newStructConverter(st, avroType)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields))

      case other =>
        throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystType to " +
          s"Avro type $avroType.")
    }
  }

  private def newStructConverter(
      catalystStruct: StructType, avroStruct: ASchema): InternalRow => GenericRecord = {
    if (avroStruct.getType != RECORD || avroStruct.getFields.size() != catalystStruct.length) {
      throw new IncompatibleSchemaException(s"Cannot convert Catalyst type $catalystStruct to " +
        s"Avro type $avroStruct.")
    }
    val fieldConverters = catalystStruct.zip(avroStruct.getFields.asScala).map {
      case (f1, f2) => newConverter(f1.dataType, resolveNullableType(f2.schema(), f1.nullable))
    }
    val numFields = catalystStruct.length
    (row: InternalRow) =>
      val pSchema = SchemaUtils.ASchema2PSchema(avroStruct)
      val builder = pSchema.newRecordBuilder()

      var i = 0
      while (i < numFields) {
        if (row.isNullAt(i)) {
          builder.set(pSchema.getFields.get(i), null)
        } else {
          builder.set(pSchema.getFields.get(i), fieldConverters(i).apply(row, i))
        }
        i += 1
      }
      builder.build()
  }

  private def resolveNullableType(avroType: ASchema, nullable: Boolean): ASchema = {
    if (nullable && avroType.getType != NULL) {
      // avro uses union to represent nullable type.
      val fields = avroType.getTypes.asScala
      assert(fields.length == 2)
      val actualType = fields.filter(_.getType != Type.NULL)
      assert(actualType.length == 1)
      actualType.head
    } else {
      avroType
    }
  }
}
