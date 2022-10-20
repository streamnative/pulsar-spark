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

import java.nio.ByteBuffer
import java.util

import scala.collection.JavaConverters._

import org.apache.pulsar.client.api.schema.Field
import org.apache.pulsar.client.impl.schema.generic.GenericAvroRecord
import org.apache.pulsar.shade.org.apache.avro.{LogicalTypes, Schema => ASchema}
import org.apache.pulsar.shade.org.apache.avro.Conversions.DecimalConversion
import org.apache.pulsar.shade.org.apache.avro.LogicalTypes.{TimestampMicros, TimestampMillis}
import org.apache.pulsar.shade.org.apache.avro.Schema.Type
import org.apache.pulsar.shade.org.apache.avro.Schema.Type._
import org.apache.pulsar.shade.org.apache.avro.generic.GenericData.EnumSymbol
import org.apache.pulsar.shade.org.apache.avro.util.Utf8

import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.SpecializedGetters
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.types._

class PulsarSerializer(rootCatalystType: DataType, nullable: Boolean) {

  def serialize(catalystData: Any): Any = {
    converter.apply(catalystData)
  }

  private lazy val decimalConversions = new DecimalConversion()
  private val rootAvroType = SchemaUtils.sqlType2ASchema(rootCatalystType)

  def getFields(aSchema: ASchema): util.List[Field] = {
    aSchema.getFields.asScala.map(f => new Field(f.name(), f.pos())).asJava
  }

  private val converter: Any => Any = {
    val actualAvroType: ASchema = resolveNullableType(rootAvroType, nullable)
    val baseConverter = rootCatalystType match {
      case st: StructType =>
        newStructConverter(st, actualAvroType).asInstanceOf[Any => Any]
      case _ =>
        val converter = singleValueConverter(rootCatalystType, actualAvroType)
        (data: Any) => converter.apply(data.asInstanceOf[SpecializedGetters], 0)
    }
    if (nullable) { (data: Any) =>
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

      case (TimestampType, LONG) =>
        avroType.getLogicalType match {
          case _: TimestampMillis =>
            (getter, ordinal) => DateTimeUtils.toJavaTimestamp(getter.getLong(ordinal) / 1000)
          case _: TimestampMicros =>
            (getter, ordinal) => DateTimeUtils.toJavaTimestamp(getter.getLong(ordinal))
          case other =>
            throw new IncompatibleSchemaException(
              s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
        }

      case other =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst type $catalystType to " +
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

      case (d: DecimalType, FIXED)
          if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        (getter, ordinal) =>
          val decimal = getter.getDecimal(ordinal, d.precision, d.scale)
          decimalConversions.toFixed(
            decimal.toJavaBigDecimal,
            avroType,
            LogicalTypes.decimal(d.precision, d.scale))

      case (d: DecimalType, BYTES)
          if avroType.getLogicalType == LogicalTypes.decimal(d.precision, d.scale) =>
        (getter, ordinal) =>
          val decimal = getter.getDecimal(ordinal, d.precision, d.scale)
          decimalConversions.toBytes(
            decimal.toJavaBigDecimal,
            avroType,
            LogicalTypes.decimal(d.precision, d.scale))

      case (BinaryType, BYTES) =>
        (getter, ordinal) => ByteBuffer.wrap(getter.getBinary(ordinal))
      case (DateType, INT) =>
        (getter, ordinal) => getter.getInt(ordinal)

      case (TimestampType, LONG) =>
        avroType.getLogicalType match {
          case _: TimestampMillis =>
            (getter, ordinal) => getter.getLong(ordinal) / 1000
          case _: TimestampMicros =>
            (getter, ordinal) => getter.getLong(ordinal)
          case other =>
            throw new IncompatibleSchemaException(
              s"Cannot convert Catalyst Timestamp type to Avro logical type ${other}")
        }

      case (StringType, STRING) =>
        (getter, ordinal) =>
          val ov = ordinal
          new Utf8(getter.getUTF8String(ordinal).getBytes)

      case (StringType, ENUM) =>
        val enumSymbols: Set[String] = avroType.getEnumSymbols.asScala.toSet
        (getter, ordinal) =>
          val data = getter.getUTF8String(ordinal).toString
          if (!enumSymbols.contains(data)) {
            throw new IncompatibleSchemaException(
              "Cannot write \"" + data + "\" since it's not defined in enum \"" +
                enumSymbols.mkString("\", \"") + "\"")
          }
          new EnumSymbol(avroType, data)

      case (ArrayType(et, containsNull), ARRAY) =>
        val elementConverter =
          newConverter(et, resolveNullableType(avroType.getElementType, containsNull))
        (getter, ordinal) => {
          val arrayData = getter.getArray(ordinal)
          val len = arrayData.numElements()
          val result = new Array[Any](len)
          var i = 0
          while (i < len) {
            if (containsNull && arrayData.isNullAt(i)) {
              result(i) = null
            } else {
              result(i) = elementConverter(arrayData, i)
            }
            i += 1
          }
          // avro writer is expecting a Java Collection, so we convert it into
          // `ArrayList` backed by the specified array without data copying.
          java.util.Arrays.asList(result: _*)
        }

      case (MapType(kt, vt, valueContainsNull), MAP) if kt == StringType =>
        val valueConverter =
          newConverter(vt, resolveNullableType(avroType.getValueType, valueContainsNull))
        (getter, ordinal) =>
          val mapData = getter.getMap(ordinal)
          val len = mapData.numElements()
          val result = new java.util.HashMap[String, Any](len)
          val keyArray = mapData.keyArray()
          val valueArray = mapData.valueArray()
          var i = 0
          while (i < len) {
            val key = keyArray.getUTF8String(i).toString
            if (valueContainsNull && valueArray.isNullAt(i)) {
              result.put(key, null)
            } else {
              result.put(key, valueConverter(valueArray, i))
            }
            i += 1
          }
          result

      case (st: StructType, RECORD) =>
        val structConverter = newStructConverter(st, avroType)
        val numFields = st.length
        (getter, ordinal) => structConverter(getter.getStruct(ordinal, numFields)).getAvroRecord

      case other =>
        throw new IncompatibleSchemaException(
          s"Cannot convert Catalyst type $catalystType to " +
            s"Avro type $avroType.")
    }
  }

  private def newStructConverter(
      catalystStruct: StructType,
      avroStruct: ASchema): InternalRow => GenericAvroRecord = {
    if (avroStruct.getType != RECORD || avroStruct.getFields.size() != catalystStruct.length) {
      throw new IncompatibleSchemaException(
        s"Cannot convert Catalyst type $catalystStruct to " +
          s"Avro type $avroStruct.")
    }
    val fieldConverters = catalystStruct.zip(avroStruct.getFields.asScala).map { case (f1, f2) =>
      newConverter(f1.dataType, resolveNullableType(f2.schema(), f1.nullable))
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
      builder.build().asInstanceOf[GenericAvroRecord]
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
