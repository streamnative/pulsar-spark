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

import org.apache.pulsar.client.impl.MessageIdImpl

import org.apache.spark.sql.execution.streaming.{
  LongOffset,
  OffsetSeq,
  OffsetSeqLog,
  SerializedOffset
}
import org.apache.spark.sql.streaming.OffsetSuite
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.execution.streaming.Offset

import java.io.File

class PulsarSourceOffsetSuite extends OffsetSuite with SharedSparkSession {

  compare(
    one = SpecificPulsarOffset(("t", new MessageIdImpl(1, 1, -1))).asInstanceOf[Offset],
    two = SpecificPulsarOffset(("t", new MessageIdImpl(1, 2, -1))).asInstanceOf[Offset])

  compare(
    one = SpecificPulsarOffset(
      ("t", new MessageIdImpl(1, 1, -1)),
      ("t1", new MessageIdImpl(1, 1, -1))).asInstanceOf[Offset],
    two = SpecificPulsarOffset(
      ("t", new MessageIdImpl(1, 2, -1)),
      ("t1", new MessageIdImpl(1, 2, -1))).asInstanceOf[Offset])

  compare(
    one = SpecificPulsarOffset(("t", new MessageIdImpl(1, 1, -1))).asInstanceOf[Offset],
    two = SpecificPulsarOffset(
      ("t", new MessageIdImpl(1, 2, -1)),
      ("t1", new MessageIdImpl(1, 1, -1))).asInstanceOf[Offset])

  val kso1 = SpecificPulsarOffset(("t", new MessageIdImpl(1, 1, -1)))
  val kso2 =
    SpecificPulsarOffset(("t", new MessageIdImpl(1, 2, -1)), ("t1", new MessageIdImpl(1, 3, -1)))
  val kso3 = SpecificPulsarOffset(
    ("t", new MessageIdImpl(1, 2, -1)),
    ("t1", new MessageIdImpl(1, 3, -1)),
    ("t2", new MessageIdImpl(1, 4, -1)))

  compare(
    SpecificPulsarOffset(SerializedOffset(kso1.json)).asInstanceOf[Offset],
    SpecificPulsarOffset(SerializedOffset(kso2.json)).asInstanceOf[Offset])

  test("basic serialization - deserialization") {
    assert(
      SpecificPulsarOffset.getTopicOffsets(kso1) ==
        SpecificPulsarOffset.getTopicOffsets(SerializedOffset(kso1.json)))
  }

  test("OffsetSeqLog serialization - deserialization") {
    withTempDir { temp =>
      // use non-existent directory to test whether log make the dir
      val dir = new File(temp, "dir")
      val metadataLog = new OffsetSeqLog(spark, dir.getAbsolutePath)
      val batch0 = OffsetSeq.fill(kso1)
      val batch1 = OffsetSeq.fill(kso2, kso3)

      val batch0Serialized =
        OffsetSeq.fill(batch0.offsets.flatMap(_.map(o => SerializedOffset(o.json))): _*)

      val batch1Serialized =
        OffsetSeq.fill(batch1.offsets.flatMap(_.map(o => SerializedOffset(o.json))): _*)

      assert(metadataLog.add(0, batch0))
      assert(metadataLog.getLatest() === Some(0 -> batch0Serialized))
      assert(metadataLog.get(0) === Some(batch0Serialized))

      assert(metadataLog.add(1, batch1))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(
        metadataLog.get(None, Some(1)) ===
          Array(0 -> batch0Serialized, 1 -> batch1Serialized))

      // Adding the same batch does nothing
      metadataLog.add(1, OffsetSeq.fill(LongOffset(3)))
      assert(metadataLog.get(0) === Some(batch0Serialized))
      assert(metadataLog.get(1) === Some(batch1Serialized))
      assert(metadataLog.getLatest() === Some(1 -> batch1Serialized))
      assert(
        metadataLog.get(None, Some(1)) ===
          Array(0 -> batch0Serialized, 1 -> batch1Serialized))
    }
  }
}
