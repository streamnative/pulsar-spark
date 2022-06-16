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


import java.io.{ByteArrayInputStream, ByteArrayOutputStream, ObjectInputStream, ObjectOutputStream}
import java.nio.charset.StandardCharsets
import java.util

import org.apache.avro.{Schema => ASchema}

import org.apache.pulsar.client.impl.schema.SchemaInfoImpl
import org.apache.pulsar.common.schema.{SchemaInfo, SchemaType}

import org.apache.spark.SparkFunSuite

class SchemaInfoSerDeSuite extends SparkFunSuite {

  test("serialized schemaInfo serde") {

    val s1 = new ASchema.Parser().parse("{\n    \"type\": \"record\",\n    \"name\": \"User\",\n    \"namespace\": \"io.streamnative.connectors.kafka.example\",\n    \"fields\": [\n      {\n        \"name\": \"txtm\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"bkid\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"gw_txtp\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"rgtm\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"first_banding_time\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"isrn\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"issucc\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"first_banding_area\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"commTransType\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"alam\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"chod\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"pdno\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"gw_trtp\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"frid\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"tcid\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"bobj\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"gscd\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"area_code\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"name_real\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"opponent_name\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"ctnm\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"registerIpAreaCode\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"idNoReal\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"chel\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      },\n      {\n        \"name\": \"idNoRealCipher\",\n        \"type\": [\n          \"string\",\n          \"null\"\n        ]\n      }\n    ]\n  }")
    val ps1 = new SchemaInfoImpl("datafromkafkatopulsar",
      s1.toString.getBytes(StandardCharsets.UTF_8),
      SchemaType.AVRO,
      new util.HashMap[String, String]())

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
