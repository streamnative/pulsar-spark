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

import java.util.concurrent.ConcurrentMap
import java.{util => ju}

import org.apache.pulsar.client.api.PulsarClient
import org.apache.spark.sql.test.SharedSQLContext
import org.junit.runner.RunWith
import org.scalatest.PrivateMethodTester
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class CachedPulsarProducerSuite extends SharedSQLContext with PrivateMethodTester with PulsarTest {

  import PulsarOptions._

  private type K = (Seq[(String, Object)], Seq[(String, Object)], String)
  private type V = org.apache.pulsar.client.api.Producer[Array[Byte]]
  type KP = PulsarClient

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CachedPulsarProducer.clear()
    CachedPulsarClient.clear()
  }

  test("Should return the cached instance on calling getOrCreate with same params.") {
    val clientConf = new ju.HashMap[String, Object]()
    clientConf.put(SERVICE_URL_OPTION_KEY, serviceUrl)
    clientConf.put("concurrentLookupRequest", "10000")

    val producerConf = new ju.HashMap[String, Object]()
    producerConf.put("batchingMaxMessages", "5")

    val producer = CachedPulsarProducer.getOrCreate((clientConf, producerConf, "a"))
    val producer2 = CachedPulsarProducer.getOrCreate((clientConf, producerConf, "a"))
    assert(producer == producer2)

    val cacheMap = PrivateMethod[ConcurrentMap[K, V]]('getAsMap)
    val map = CachedPulsarProducer.invokePrivate(cacheMap())
    assert(map.size == 1)
  }

  test("Should close the correct pulsar producer for the given pulsarPrams.") {
    val clientConf = new ju.HashMap[String, Object]()
    clientConf.put(SERVICE_URL_OPTION_KEY, serviceUrl)
    clientConf.put("concurrentLookupRequest", "10000")

    val producerConf = new ju.HashMap[String, Object]()
    producerConf.put("batchingMaxMessages", "5")

    val producer = CachedPulsarProducer.getOrCreate((clientConf, producerConf, "a"))
    clientConf.put("concurrentLookupRequest", "20000")
    val producer2 = CachedPulsarProducer.getOrCreate((clientConf, producerConf, "a"))
    assert(producer != producer2)

    producerConf.put("batchingMaxMessages", "6")
    val producer3 = CachedPulsarProducer.getOrCreate((clientConf, producerConf, "a"))
    assert(producer2 != producer3)

    val producer4 = CachedPulsarProducer.getOrCreate((clientConf, producerConf, "b"))
    assert(producer3 != producer4)

    clientConf.put("concurrentLookupRequest", "300")
    val producer5 = CachedPulsarProducer.getOrCreate((clientConf, producerConf, "b"))
    assert(producer4 != producer5)


    val cacheMap = PrivateMethod[ConcurrentMap[K, V]]('getAsMap)
    val map = CachedPulsarProducer.invokePrivate(cacheMap())
    assert(map.size == 5)

    val cacheMap2 = PrivateMethod[ConcurrentMap[Seq[(String, Object)], KP]]('getAsMap)
    val map2 = CachedPulsarClient.invokePrivate(cacheMap2())
    assert(map2.size == 3)

    CachedPulsarProducer.close((clientConf, producerConf, "b"))
    val map3 = CachedPulsarProducer.invokePrivate(cacheMap())
    assert(map3.size == 4)
  }
}
