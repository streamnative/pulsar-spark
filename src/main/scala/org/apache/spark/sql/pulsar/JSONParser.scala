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

import java.io.{ByteArrayOutputStream, CharConversionException}
import java.nio.charset.MalformedInputException

import scala.collection.mutable.ArrayBuffer
import scala.util.Try

import com.fasterxml.jackson.core._

import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.GenericInternalRow
import org.apache.spark.sql.catalyst.json.{JacksonUtils, JSONOptions}
import org.apache.spark.sql.catalyst.util._
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.Utils

private[sql] object CreateJacksonRecordParser extends Serializable {

  def string(jsonFactory: JsonFactory, record: String): JsonParser = {
    jsonFactory.createParser(record)
  }
}

class JacksonRecordParser(schema: DataType, val options: JSONOptions) extends Logging {

  import JacksonUtils._
  import com.fasterxml.jackson.core.JsonToken._

  assert(schema.isInstanceOf[StructType])

  // A `ValueConverter` is responsible for converting a value from `JsonParser`
  // to a value in a field for `InternalRow`.
  private type ValueConverter = JsonParser => AnyRef

  // `ValueConverter`s for the root schema for all fields in the schema
  private val rootConverter =
    makeStructRootConverter(schema.asInstanceOf[StructType])

  private val factory = options.buildJsonFactory()

  private def makeStructRootConverter(
      st: StructType): (JsonParser, InternalRow) => InternalRow = {
    val fieldConverters = st.map(_.dataType).map(makeConverter).toArray
    (parser: JsonParser, row: InternalRow) =>
      parseJsonToken[InternalRow](parser, st) {
        case START_OBJECT => convertObject(parser, st, fieldConverters, row)
        case START_ARRAY =>
          throw new IllegalStateException("Message should be a single JSON object")
      }
  }

  /**
   * Create a converter which converts the JSON documents held by the `JsonParser` to a value
   * according to a desired schema.
   */
  def makeConverter(dataType: DataType): ValueConverter = dataType match {
    case BooleanType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Boolean](parser, dataType) {
          case VALUE_TRUE => true
          case VALUE_FALSE => false
        }

    case ByteType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Byte](parser, dataType) { case VALUE_NUMBER_INT =>
          parser.getByteValue
        }

    case ShortType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Short](parser, dataType) { case VALUE_NUMBER_INT =>
          parser.getShortValue
        }

    case IntegerType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Integer](parser, dataType) { case VALUE_NUMBER_INT =>
          parser.getIntValue
        }

    case LongType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Long](parser, dataType) { case VALUE_NUMBER_INT =>
          parser.getLongValue
        }

    case FloatType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Float](parser, dataType) {
          case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
            parser.getFloatValue

          case VALUE_STRING =>
            // Special case handling for NaN and Infinity.
            parser.getText match {
              case "NaN" => Float.NaN
              case "Infinity" => Float.PositiveInfinity
              case "-Infinity" => Float.NegativeInfinity
              case other =>
                throw new RuntimeException(s"Cannot parse $other as ${FloatType.catalogString}.")
            }
        }

    case DoubleType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Double](parser, dataType) {
          case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
            parser.getDoubleValue

          case VALUE_STRING =>
            // Special case handling for NaN and Infinity.
            parser.getText match {
              case "NaN" => Double.NaN
              case "Infinity" => Double.PositiveInfinity
              case "-Infinity" => Double.NegativeInfinity
              case other =>
                throw new RuntimeException(s"Cannot parse $other as ${DoubleType.catalogString}.")
            }
        }

    case StringType =>
      (parser: JsonParser) =>
        parseJsonToken[UTF8String](parser, dataType) {
          case VALUE_STRING =>
            UTF8String.fromString(parser.getText)

          case _ =>
            // Note that it always tries to convert the data as string without the case of failure.
            val writer = new ByteArrayOutputStream()
            Utils.tryWithResource(factory.createGenerator(writer, JsonEncoding.UTF8)) {
              generator =>
                generator.copyCurrentStructure(parser)
            }
            UTF8String.fromBytes(writer.toByteArray)
        }

    case TimestampType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Long](parser, dataType) {
          case VALUE_STRING =>
            val stringValue = parser.getText
            // This one will lose microseconds parts.
            // See https://issues.apache.org/jira/browse/SPARK-10681.
            Long.box {
              Try(
                TimestampFormatter(options.timestampFormat, options.zoneId, true)
                  .parse(stringValue))
                .getOrElse {
                  // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
                  // compatibility.
                  DateTimeUtils
                    .stringToTimestamp(UTF8String.fromString(stringValue), options.zoneId)
                    .get
                }
            }

          case VALUE_NUMBER_INT =>
            parser.getLongValue * 1000000L
        }

    case DateType =>
      (parser: JsonParser) =>
        parseJsonToken[java.lang.Long](parser, dataType) { case VALUE_STRING =>
          val stringValue = parser.getText
          // This one will lose microseconds parts.
          // See https://issues.apache.org/jira/browse/SPARK-10681.x
          Long.box {
            Try(
              TimestampFormatter(options.timestampFormat, options.zoneId, true).parse(
                stringValue))
              .orElse {
                // If it fails to parse, then tries the way used in 2.0 and 1.x for backwards
                // compatibility.
                Try(
                  DateTimeUtils
                    .stringToTimestamp(UTF8String.fromString(stringValue), options.zoneId)
                    .get)
              }
              .getOrElse {
                // In Spark 1.5.0, we store the data as number of days since epoch in string.
                // So, we just convert it to Int.
                stringValue.toLong
              }
          }
        }

    case BinaryType =>
      (parser: JsonParser) =>
        parseJsonToken[Array[Byte]](parser, dataType) { case VALUE_STRING =>
          parser.getBinaryValue
        }

    case dt: DecimalType =>
      (parser: JsonParser) =>
        parseJsonToken[Decimal](parser, dataType) {
          case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT) =>
            Decimal(parser.getDecimalValue, dt.precision, dt.scale)
        }

    case st: StructType =>
      val fieldConverters = st.map(_.dataType).map(makeConverter).toArray
      (parser: JsonParser) =>
        parseJsonToken[InternalRow](parser, dataType) { case START_OBJECT =>
          val record = new GenericInternalRow(st.length)
          convertObject(parser, st, fieldConverters, record)
        }

    case at: ArrayType =>
      val elementConverter = makeConverter(at.elementType)
      (parser: JsonParser) =>
        parseJsonToken[ArrayData](parser, dataType) { case START_ARRAY =>
          convertArray(parser, elementConverter)
        }

    case mt: MapType =>
      val valueConverter = makeConverter(mt.valueType)
      (parser: JsonParser) =>
        parseJsonToken[MapData](parser, dataType) { case START_OBJECT =>
          convertMap(parser, valueConverter)
        }

    case udt: UserDefinedType[_] =>
      makeConverter(udt.sqlType)

    case _ =>
      (parser: JsonParser) =>
        // Here, we pass empty `PartialFunction` so that this case can be
        // handled as a failed conversion. It will throw an exception as
        // long as the value is not null.
        parseJsonToken[AnyRef](parser, dataType)(PartialFunction.empty[JsonToken, AnyRef])
  }

  /**
   * This method skips `FIELD_NAME`s at the beginning, and handles nulls ahead before trying to
   * parse the JSON token using given function `f`. If the `f` failed to parse and convert the
   * token, call `failedConversion` to handle the token.
   */
  private def parseJsonToken[R >: Null](parser: JsonParser, dataType: DataType)(
      f: PartialFunction[JsonToken, R]): R = {
    parser.getCurrentToken match {
      case FIELD_NAME =>
        // There are useless FIELD_NAMEs between START_OBJECT and END_OBJECT tokens
        parser.nextToken()
        parseJsonToken[R](parser, dataType)(f)

      case null | VALUE_NULL => null

      case other => f.applyOrElse(other, failedConversion(parser, dataType))
    }
  }

  /**
   * This function throws an exception for failed conversion, but returns null for empty string,
   * to guard the non string types.
   */
  private def failedConversion[R >: Null](
      parser: JsonParser,
      dataType: DataType): PartialFunction[JsonToken, R] = {
    case VALUE_STRING if parser.getTextLength < 1 =>
      // If conversion is failed, this produces `null` rather than throwing exception.
      // This will protect the mismatch of types.
      null

    case token =>
      // We cannot parse this token based on the given data type. So, we throw a
      // RuntimeException and this exception will be caught by `parse` method.
      throw new RuntimeException(
        s"Failed to parse a value for data type ${dataType.catalogString} (current token: $token).")
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema. Fields in the
   * json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(
      parser: JsonParser,
      schema: StructType,
      fieldConverters: Array[ValueConverter],
      row: InternalRow): InternalRow = {
    val allFields = 0 until schema.fields.length
    val nullFields = collection.mutable.Set(allFields: _*)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      schema.getFieldIndex(parser.getCurrentName) match {
        case Some(index) =>
          nullFields.remove(index)
          row.update(index, fieldConverters(index).apply(parser))

        case None =>
          parser.skipChildren()
      }
    }
    nullFields.foreach(row.setNullAt)

    row
  }

  /**
   * Parse an object as a Map, preserving all fields.
   */
  private def convertMap(parser: JsonParser, fieldConverter: ValueConverter): MapData = {
    val keys = ArrayBuffer.empty[UTF8String]
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      keys += UTF8String.fromString(parser.getCurrentName)
      values += fieldConverter.apply(parser)
    }

    ArrayBasedMapData(keys.toArray, values.toArray)
  }

  /**
   * Parse an object as a Array.
   */
  private def convertArray(parser: JsonParser, fieldConverter: ValueConverter): ArrayData = {
    val values = ArrayBuffer.empty[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      values += fieldConverter.apply(parser)
    }

    new GenericArrayData(values.toArray)
  }

  /**
   * Parse the JSON input to [[InternalRow]].
   *
   * @param recordLiteral
   *   an optional function that will be used to generate the corrupt record text instead of
   *   record.toString
   */
  def parse[T](
      record: T,
      createParser: (JsonFactory, T) => JsonParser,
      recordLiteral: T => UTF8String,
      row: InternalRow): InternalRow = {
    try {
      Utils.tryWithResource(createParser(factory, record)) { parser =>
        // a null first token is equivalent to testing for input.trim.isEmpty
        // but it works on any token stream and not just strings
        parser.nextToken() match {
          case null => InternalRow.empty
          case _ =>
            rootConverter.apply(parser, row) match {
              case null => throw new RuntimeException("Root converter returned null")
              case row => row
            }
        }
      }
    } catch {
      case e @ (_: RuntimeException | _: JsonProcessingException | _: MalformedInputException) =>
        // JSON parser currently doesn't support partial results for corrupted records.
        // For such records, all fields other than the meta fields are set to `null`.
        throw BadRecordException(() => recordLiteral(record), () => None, e)
      case e: CharConversionException if options.encoding.isEmpty =>
        val msg =
          """JSON parser cannot handle a character in its input.
            |Specifying encoding as an input option explicitly might help to resolve the issue.
            |""".stripMargin + e.getMessage
        val wrappedCharException = new CharConversionException(msg)
        wrappedCharException.initCause(e)
        throw BadRecordException(() => recordLiteral(record), () => None, wrappedCharException)
    }
  }
}

class FailureSafeRecordParser[IN](
    rawParser: (IN, InternalRow) => InternalRow,
    mode: ParseMode,
    schema: StructType) {

  def parse(input: IN, row: InternalRow): InternalRow = {
    try {
      rawParser.apply(input, row)
    } catch {
      case e: BadRecordException =>
        mode match {
          case PermissiveMode =>
            row
          case DropMalformedMode =>
            null
          case FailFastMode =>
            throw new SparkException(
              "Malformed records are detected in record parsing. " +
                s"Parse Mode: ${FailFastMode.name}.",
              e.cause)
        }
    }
  }
}
