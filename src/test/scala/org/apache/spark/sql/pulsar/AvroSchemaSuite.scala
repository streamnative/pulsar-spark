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

import java.math.BigDecimal
import java.util.concurrent.atomic.AtomicInteger

import org.apache.pulsar.common.naming.TopicName

import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.{DataFrame, QueryTest}

case class IS(i: Int, s: String)
case class MapFoo(m1: Map[String, Int], m2: Map[String, IS])
case class ArrayFoo(a1: Array[IS], a2: Array[String])
case class BDFoo(b: BigDecimal, c: Int = 1)

class AvroSchemaSuite extends QueryTest with SharedSparkSession with PulsarTest {
  import PulsarOptions._
  import testImplicits._

  test("batch - avro map") {
    val mapFooSeq = MapFoo(null, null) :: MapFoo(Map(), Map()) ::
      MapFoo(Map("x" -> 1, "y" -> 2), Map("w" -> IS(1, "1"), "H" -> IS(2, "2"))) :: Nil

    val topic = newTopic()

    val df = mapFooSeq.toDF()

    df.write
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)
      .mode("append")
      .save()

    val df1 = createPulsarReader(topic).selectExpr("m1", "m2")

    checkAnswer(df1, df)
  }

  test("batch - avro array") {
    val arrayFoo = ArrayFoo(null, null) :: ArrayFoo(Array.empty[IS], Array.empty[String]) ::
      ArrayFoo(Array(IS(1, "1"), IS(2, "2")), Array("1", "2", "3")) :: Nil

    val topic = newTopic()

    val df = arrayFoo.toDF()

    df.write
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)
      .mode("append")
      .save()

    val df1 = createPulsarReader(topic).selectExpr("a1", "a2")

    checkAnswer(df1, df)
  }

  test("batch - avro bigDecimal") {
    val bdSeq = BDFoo(new BigDecimal("0")) :: BDFoo(new BigDecimal("0.00")) :: BDFoo(
      new BigDecimal("123")) ::
      BDFoo(new BigDecimal("-123")) :: BDFoo(new BigDecimal("1.23E3")) :: BDFoo(
      new BigDecimal("1.23E+3")) ::
      BDFoo(new BigDecimal("12.3E+7")) :: BDFoo(new BigDecimal("12.0")) :: BDFoo(
      new BigDecimal("12.3")) ::
      BDFoo(new BigDecimal("0.00123")) :: BDFoo(new BigDecimal("-1.23E-12")) :: BDFoo(
      new BigDecimal("1234.5E-4")) ::
      BDFoo(new BigDecimal("0E+7")) :: BDFoo(new BigDecimal("-0")) :: Nil

    val topic = newTopic()

    val df = bdSeq.toDF()

    df.write
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(TOPIC_SINGLE, topic)
      .mode("append")
      .save()

    val df1 = createPulsarReader(topic).selectExpr("b", "c")

    checkAnswer(df1, df)
  }

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = TopicName.get(s"topic-${topicId.getAndIncrement()}").toString

  private def createPulsarReader(topic: String): DataFrame = {
    spark.read
      .format("pulsar")
      .option(SERVICE_URL_OPTION_KEY, serviceUrl)
      .option(ADMIN_URL_OPTION_KEY, adminUrl)
      .option(STARTING_OFFSETS_OPTION_KEY, "earliest")
      .option(ENDING_OFFSETS_OPTION_KEY, "latest")
      .option(TOPIC_SINGLE, topic)
      .load()
  }

}
