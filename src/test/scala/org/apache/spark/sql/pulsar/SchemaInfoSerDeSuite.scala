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

import org.apache.avro.{Schema => ASchema}
import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.SchemaType
import org.apache.spark.SparkFunSuite

import java.io.{
  ByteArrayInputStream,
  ByteArrayOutputStream,
  ObjectInputStream,
  ObjectOutputStream
}
import java.nio.charset.StandardCharsets
import java.util

class SchemaInfoSerDeSuite extends SparkFunSuite {

  val schemaJson =
    """{
      |  "type": "record",
      |  "name": "User",
      |  "namespace": "io.streamnative.connectors.kafka.example",
      |  "fields": [
      |    {
      |      "name": "txtm",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "bkid",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "gw_txtp",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "rgtm",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "first_banding_time",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "isrn",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "issucc",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "first_banding_area",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "commTransType",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "alam",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "chod",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "pdno",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "gw_trtp",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "frid",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "tcid",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "bobj",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "gscd",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "area_code",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "name_real",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "opponent_name",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "ctnm",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "registerIpAreaCode",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "idNoReal",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "chel",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    },
      |    {
      |      "name": "idNoRealCipher",
      |      "type": [
      |        "string",
      |        "null"
      |      ]
      |    }
      |  ]
      |}""".stripMargin

  test("serialized schemaInfo serde") {
    val s1 = new ASchema.Parser().parse(schemaJson)

    val ps1 = SchemaInfoImpl
      .builder()
      .name("datafromkafkatopulsar")
      .schema(s1.toString.getBytes(StandardCharsets.UTF_8))
      .`type`(SchemaType.AVRO)
      .build()

    val serializedPS1 = new SchemaInfoSerializable(ps1)

    val out = new ByteArrayOutputStream()
    val oos = new ObjectOutputStream(out)
    oos.writeObject(serializedPS1)

    val in = new ByteArrayInputStream(out.toByteArray)
    val ois = new ObjectInputStream(in)
    val bb = ois.readObject().asInstanceOf[SchemaInfoSerializable]

    assert(ps1 == bb.si)
  }
}
