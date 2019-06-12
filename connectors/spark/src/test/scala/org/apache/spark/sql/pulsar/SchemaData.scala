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

import java.sql.Timestamp
import java.util.Date

object SchemaData {

  val booleanSeq = Seq(true, false, true, true, false)
  val bytesSeq = 1.to(5).map(_.toString.getBytes)
  val dateSeq = Seq(new Date(119, 1, 1), new Date(119, 1, 2), new Date(119, 1, 3), new Date(119, 1, 4), new Date(119, 1, 5))
  val stringSeq = 1.to(5).map(_.toString)
  val timestampSeq = Seq(
    new Timestamp(119, 1, 1, 20, 35, 40, 10),
    new Timestamp(119, 1, 2, 20, 35, 40, 10),
    new Timestamp(119, 1, 3, 20, 35, 40, 10),
    new Timestamp(119, 1, 4, 20, 35, 40, 10),
    new Timestamp(119, 1, 5, 20, 35, 40, 10))
  val int8Seq = 1.to(5).map(_.toByte)
  val doubleSeq = 1.to(5).map(_.toDouble)
  val floatSeq = 1.to(5).map(_.toFloat)
  val int32Seq = 1.to(5)
  val int64Seq = 1.to(5).map(_.toLong)
  val int16Seq = 1.to(5).map(_.toShort)

  case class Foo(i: Int, f: Float, bar: Bar)
  case class Bar(b: Boolean, s: String)

  val fooSeq: Seq[Foo] =
    Foo(1, 1.0.toFloat, Bar(true, "a")) :: Foo(2, 2.0.toFloat, Bar(false, "b")) :: Foo(3, 0, null) :: Nil
}
