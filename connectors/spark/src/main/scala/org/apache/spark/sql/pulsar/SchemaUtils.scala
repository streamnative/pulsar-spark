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

import java.io.{Externalizable, ObjectInput, ObjectOutput}
import java.nio.charset.StandardCharsets
import java.nio.charset.StandardCharsets.UTF_8

import scala.collection.JavaConverters._
import scala.collection.mutable.ListBuffer
import org.apache.avro.{LogicalTypes, SchemaBuilder, Schema => ASchema}
import org.apache.avro.Schema.Type
import org.apache.avro.LogicalTypes.{Date, TimestampMicros, TimestampMillis}
import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.pulsar.client.admin.{PulsarAdmin, PulsarAdminException}
import org.apache.pulsar.client.api.schema.{GenericRecord, GenericSchema}
import org.apache.pulsar.client.api.{Schema => PSchema}
import org.apache.pulsar.client.impl.schema._
import org.apache.pulsar.client.impl.schema.generic.GenericSchemaImpl
import org.apache.pulsar.common.naming.TopicName
import org.apache.pulsar.common.schema.{PostSchemaPayload, SchemaInfo, SchemaType}
import org.apache.spark.sql.types._

class IncompatibleSchemaException(msg: String, ex: Throwable = null) extends Exception(msg, ex)

class SchemaInfoSerializable(var si: SchemaInfo) extends Externalizable {

  def this() = this(null)  // For deserialization only

  override def writeExternal(out: ObjectOutput): Unit = {
    val schema = si.getSchema
    if (schema.length == 0) {
      out.writeInt(0)
    } else {
      out.writeInt(schema.length)
      out.write(schema)
    }

    out.writeUTF(si.getName)
    out.writeObject(si.getProperties)
    out.writeInt(si.getType.getValue)
  }

  override def readExternal(in: ObjectInput): Unit = {
    si = new SchemaInfo()
    val len = in.readInt()
    if (len > 0) {
      val ba = new Array[Byte](len)
      in.read(ba)
      si.setSchema(ba)
    } else {
      si.setSchema(new Array[Byte](0))
    }
    si.setName(in.readUTF())
    si.setProperties(in.readObject().asInstanceOf[java.util.Map[String, String]])
    si.setType(SchemaType.valueOf(in.readInt()))
  }
}

private[pulsar] object SchemaUtils {

  private lazy val nullSchema = ASchema.create(ASchema.Type.NULL)

  def uploadPulsarSchema(admin: PulsarAdmin, topic: String, schemaInfo: SchemaInfo): Unit = {
    assert(schemaInfo != null, "schemaInfo shouldn't be null")

    val existingSchema = try {
      admin.schemas().getSchemaInfo(TopicName.get(topic).toString)
    } catch {
      case e: PulsarAdminException if e.getStatusCode == 404 =>
        null
      case e: Throwable => throw new RuntimeException(
        s"Failed to get schema information for ${TopicName.get(topic).toString}: " +
          ExceptionUtils.getRootCause(e).getLocalizedMessage, e)
    }

    if (existingSchema == null) {
      val pl = new PostSchemaPayload()
      pl.setType(schemaInfo.getType.name())
      pl.setSchema(new String(schemaInfo.getSchema, UTF_8))
      pl.setProperties(schemaInfo.getProperties)
      try {
        admin.schemas().createSchema(TopicName.get(topic).toString, pl)
      } catch {
        case e: PulsarAdminException if e.getStatusCode == 404 =>
          throw new RuntimeException(s"Create schema for ${TopicName.get(topic).toString} got 404")
        case e: Throwable => throw new RuntimeException(
          s"Failed to create schema for ${TopicName.get(topic).toString}: " +
            ExceptionUtils.getRootCause(e).getLocalizedMessage, e)
      }
    } else if (existingSchema.equals(schemaInfo) || compatibleSchema(existingSchema, schemaInfo)) {
      // no need to upload again
    } else {
      throw new RuntimeException("Writing to a topic which have incompatible schema")
    }
  }

  def compatibleSchema(x: SchemaInfo, y: SchemaInfo): Boolean = {
    (x.getType.getValue, y.getType.getValue) match {
      // None and bytes are compatible
      case (0, -1) => true
      case (-1, 0) => true
      case _ => false
    }
  }

  def emptySchemaInfo(): SchemaInfo = {
    SchemaInfo.builder().name("empty").`type`(SchemaType.NONE).schema(new Array[Byte](0)).build()
  }

  def getPSchema(schemaInfo: SchemaInfo): PSchema[_] = schemaInfo.getType match {
    case SchemaType.BOOLEAN =>
      BooleanSchema.of()
    case SchemaType.INT8 =>
      ByteSchema.of()
    case SchemaType.INT16 =>
      ShortSchema.of()
    case SchemaType.INT32 =>
      IntSchema.of()
    case SchemaType.INT64 =>
      LongSchema.of()
    case SchemaType.STRING =>
      StringSchema.utf8()
    case SchemaType.FLOAT =>
      FloatSchema.of()
    case SchemaType.DOUBLE =>
      DoubleSchema.of()
    case SchemaType.BYTES =>
      BytesSchema.of()
    case SchemaType.DATE =>
      DateSchema.of()
    case SchemaType.TIME =>
      TimeSchema.of()
    case SchemaType.TIMESTAMP =>
      TimestampSchema.of()
    case SchemaType.NONE =>
      BytesSchema.of()
    case SchemaType.AVRO =>
      GenericSchemaImpl.of(schemaInfo)
    case _ =>
      throw new IllegalArgumentException("Retrieve schema instance from schema info for type '" +
        schemaInfo.getType + "' is not supported yet")
  }

  def pulsarSourceSchema(si: SchemaInfo): StructType = {
    var mainSchema: ListBuffer[StructField] = ListBuffer.empty
    val sqlType = si2SqlType(si)
    sqlType match {
      case st: StructType =>
        mainSchema ++=  st.fields
      case t =>
        mainSchema += StructField("value", t)
    }
    mainSchema ++= metaDataFields
    StructType(mainSchema)
  }

  def si2SqlType(si: SchemaInfo): DataType = {
    si.getType match {
      case SchemaType.NONE => BinaryType
      case SchemaType.BOOLEAN => BooleanType
      case SchemaType.BYTES => BinaryType
      case SchemaType.DATE => DateType
      case SchemaType.STRING => StringType
      case SchemaType.TIMESTAMP => TimestampType
      case SchemaType.INT8 => ByteType
      case SchemaType.DOUBLE => DoubleType
      case SchemaType.FLOAT => FloatType
      case SchemaType.INT32 => IntegerType
      case SchemaType.INT64 => LongType
      case SchemaType.INT16 => ShortType
      case SchemaType.AVRO =>
        val avroSchema: ASchema = new org.apache.avro.Schema.Parser().parse(
          new String(si.getSchema, StandardCharsets.UTF_8))
        avro2SqlType(avroSchema, Set.empty)
      case si =>
        throw new NotImplementedError(s"We do not support $si currently.")
    }
  }

  def avro2SqlType(as: ASchema, existingRecordNames: Set[String]): DataType = {
    as.getType match {
      case Type.BOOLEAN => BooleanType
      case Type.BYTES => BinaryType
      case Type.STRING => StringType
      case Type.DOUBLE => DoubleType
      case Type.FLOAT => FloatType
      case Type.INT => as.getLogicalType match {
        case _: Date => DateType
        case _ => IntegerType
      }
      case Type.LONG => as.getLogicalType match {
        case _: TimestampMillis | _: TimestampMicros => TimestampType
        case _ => LongType
      }
      case Type.RECORD =>
        if (existingRecordNames.contains(as.getFullName)) {
          throw new IncompatibleSchemaException(s"""
            |Found recursive reference in Avro schema, which can not be processed by Spark:
            |${as.toString(true)}
          """.stripMargin)
        }
        // avro will skip java built-in classes
        // org.apache.avro.reflect.ReflectData.getFields#730 ----- skip java built-in classes
        if (as.getFields.size() == 0) {
          throw new NotImplementedError(s"${as.getFullName} is not supported yet")
        } else {
          val newRecordNames = existingRecordNames + as.getFullName
          val fields = as.getFields.asScala.map { f =>
            val schemaType = avro2SqlType(f.schema(), newRecordNames)
            StructField(f.name, schemaType)
          }
          StructType(fields.toArray)
        }
      case Type.UNION =>
        if (as.getTypes.asScala.exists(_.getType == Type.NULL)) {
          // In case of a union with null, eliminate it and make a recursive call
          val remainingUnionTypes = as.getTypes.asScala.filterNot(_.getType == Type.NULL)
          if (remainingUnionTypes.size == 1) {
            avro2SqlType(remainingUnionTypes.head, existingRecordNames)
          } else {
            throw new IncompatibleSchemaException(s"Unsupported type $as")
          }
        } else null
      case other => throw new IncompatibleSchemaException(s"Unsupported type $other")
    }
  }

  def ASchema2PSchema(aschema: ASchema): GenericSchema[GenericRecord] = {
    val schema = aschema.toString.getBytes(StandardCharsets.UTF_8)
    val si = new SchemaInfo()
    si.setName("Avro")
    si.setSchema(schema)
    si.setType(SchemaType.AVRO)
    PSchema.generic(si)
  }

  def sqlType2PSchema(
      catalystType: DataType,
      nullable: Boolean = false): PSchema[_] = {

    catalystType match {
      case BooleanType => BooleanSchema.of()
      case BinaryType => BytesSchema.of()
      case DateType => DateSchema.of()
      case StringType => StringSchema.utf8()
      case TimestampType => TimestampSchema.of()
      case ByteType => ByteSchema.of()
      case DoubleType => DoubleSchema.of()
      case FloatType => FloatSchema.of()
      case IntegerType => IntSchema.of()
      case LongType => LongSchema.of()
      case ShortType => ShortSchema.of()
      case st: StructType => ASchema2PSchema(sqlType2ASchema(catalystType))
    }
  }

  // adapted from org.apache.spark.sql.avro.SchemaConverters#toAvroType
  def sqlType2ASchema(
      catalystType: DataType,
      nullable: Boolean = false,
      recordName: String = "topLevelRecord",
      nameSpace: String = ""): ASchema = {
    val builder = SchemaBuilder.builder()

    val schema = catalystType match {
      case BooleanType => builder.booleanType()
      case ByteType | ShortType | IntegerType => builder.intType()
      case LongType => builder.longType()
      case DateType =>
        LogicalTypes.date().addToSchema(builder.intType())
      case TimestampType =>
        LogicalTypes.timestampMicros().addToSchema(builder.longType())

      case FloatType => builder.floatType()
      case DoubleType => builder.doubleType()
      case StringType => builder.stringType()
      case BinaryType => builder.bytesType()

      case st: StructType =>
        val childNameSpace = if (nameSpace != "") s"$nameSpace.$recordName" else recordName
        val fieldsAssembler = builder.record(recordName).namespace(nameSpace).fields()
        st.foreach { f =>
          val fieldAvroType =
            sqlType2ASchema(f.dataType, f.nullable, f.name, childNameSpace)
          fieldsAssembler.name(f.name).`type`(fieldAvroType).noDefault()
        }
        fieldsAssembler.endRecord()

      case d: DecimalType =>
        throw new IllegalStateException("DecimalType not supported yet")
      case ArrayType(et, containsNull) =>
        throw new IllegalStateException("ArrayType not supported yet")
      case MapType(StringType, vt, valueContainsNull) =>
        throw new IllegalStateException("MapType not supported yet")

      // This should never happen.
      case other => throw new IncompatibleSchemaException(s"Unexpected type $other.")
    }
    if (nullable) {
      ASchema.createUnion(schema, nullSchema)
    } else {
      schema
    }
  }

  import PulsarOptions._

  val metaDataFields: Seq[StructField] = Seq(
    StructField(KEY_ATTRIBUTE_NAME, BinaryType),
    StructField(TOPIC_ATTRIBUTE_NAME, StringType),
    StructField(MESSAGE_ID_NAME, BinaryType),
    StructField(PUBLISH_TIME_NAME, TimestampType),
    StructField(EVENT_TIME_NAME, TimestampType)
  )

}
