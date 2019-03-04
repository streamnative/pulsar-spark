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

import java.util.concurrent.atomic.AtomicInteger

import org.apache.pulsar.segment.test.common.PulsarServiceResource
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamTest, StreamingQuery}
import org.apache.spark.sql.test.SharedSQLContext
import org.scalatest.time.SpanSugar._

class PulsarSinkSuite extends StreamTest with SharedSQLContext with PulsarTest {
  import testImplicits._

  protected var pulsarResource: PulsarServiceResource = _

  override val streamingTimeout = 30.seconds

  override def beforeAll(): Unit = {
    super.beforeAll()
    pulsarResource = new PulsarServiceResource()
    pulsarResource.setup()
  }

  override def afterAll(): Unit = {
    try {
      if (pulsarResource != null) {
        pulsarResource.teardown()
        pulsarResource = null
      }
    } finally {
      super.afterAll()
    }
  }

  /** FIXME: required to implement CreatableRelationProvider
  test("batch - write to pulsar") {
    val topic = newTopic()
    val df = Seq("1", "2", "3", "4", "5") map { v =>
      (topic, v)
    } toDF("topic", "value")
    df.write
      .format("pulsar")
      .option("spark.pulsar.serviceUrl", pulsarResource.getBrokerServiceUrl)
      .option("spark.pulsar.topic", topic)
      .save()
  }
    **/

  private val topicId = new AtomicInteger(0)

  private def newTopic(): String = s"topic-${topicId.getAndIncrement()}"

  private def createPulsarWriter(
      input: DataFrame,
      withTopic: Option[String] = None,
      withOutputMode: Option[OutputMode] = None,
      withOptions: Map[String, String] = Map[String, String]())
      (withSelectExpr: String*): StreamingQuery = {
    var stream: DataStreamWriter[Row] = null
    withTempDir { checkpointDir =>
      var df = input.toDF()
      if (withSelectExpr.length > 0) {
        df = df.selectExpr(withSelectExpr: _*)
      }
      stream = df.writeStream
        .format("pulsar")
        .option("checkpointLocation", checkpointDir.getCanonicalPath)
        .option("spark.pulsar.serviceUrl", pulsarResource.getBrokerServiceUrl)
        .queryName("pulsarStream")
      withTopic.foreach(stream.option("topic", _))
      withOutputMode.foreach(stream.outputMode(_))
      withOptions.foreach(opt => stream.option(opt._1, opt._2))
    }
    stream.start()
  }

}
