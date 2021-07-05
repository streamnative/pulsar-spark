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

import java.{util => ju}
import java.util.concurrent.ConcurrentMap

import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito.{verify, when}

import org.scalatest.PrivateMethodTester
import org.scalatest.mockito.MockitoSugar.mock

import org.apache.pulsar.client.api.{ClientBuilder, PulsarClient}

import org.apache.spark.sql.test.SharedSQLContext

class CachedPulsarClientSuite extends SharedSQLContext with PrivateMethodTester with PulsarTest {

  import PulsarOptions._

  type KP = PulsarClient

  protected override def beforeEach(): Unit = {
    super.beforeEach()
    CachedPulsarClient.clear()
  }

  test("Should return the cached instance on calling getOrCreate with same params.") {
    val pulsarParams = new ju.HashMap[String, Object]()
    // Here only host should be resolvable, it does not need a running instance of pulsar server.
    pulsarParams.put(SERVICE_URL_OPTION_KEY, "pulsar://127.0.0.1:6650")
    pulsarParams.put("concurrentLookupRequest", "10000")
    val producer = CachedPulsarClient.getOrCreate(pulsarParams)
    val producer2 = CachedPulsarClient.getOrCreate(pulsarParams)
    assert(producer == producer2)

    val cacheMap = PrivateMethod[ConcurrentMap[Seq[(String, Object)], KP]]('getAsMap)
    val map = CachedPulsarClient.invokePrivate(cacheMap())
    assert(map.size == 1)
  }

  test("Should close the correct pulsar producer for the given pulsarPrams.") {
    val pulsarParams = new ju.HashMap[String, Object]()
    pulsarParams.put(SERVICE_URL_OPTION_KEY, "pulsar://127.0.0.1:6650")
    pulsarParams.put("concurrentLookupRequest", "10000")
    val producer: KP = CachedPulsarClient.getOrCreate(pulsarParams)
    pulsarParams.put("concurrentLookupRequest", "20000")
    val producer2: KP = CachedPulsarClient.getOrCreate(pulsarParams)
    // With updated conf, a new producer instance should be created.
    assert(producer != producer2)

    val cacheMap = PrivateMethod[ConcurrentMap[Seq[(String, Object)], KP]]('getAsMap)
    val map = CachedPulsarClient.invokePrivate(cacheMap())
    assert(map.size == 2)

    CachedPulsarClient.close(pulsarParams)
    val map2 = CachedPulsarClient.invokePrivate(cacheMap())
    assert(map2.size == 1)
    import scala.collection.JavaConverters._
    val (seq: Seq[(String, Object)], _producer: KP) = map2.asScala.toArray.apply(0)
    assert(_producer == producer)
  }

  test("Should pass on authentication related parameters when authentication plugin is provided") {
    val pulsarParams = new ju.HashMap[String, Object]()
    pulsarParams.put(AUTH_PLUGIN_CLASS_NAME, "unit.test.TestAuthenticationPlugin")
    pulsarParams.put(AUTH_PARAMS, "token:abc.def.ghi")
    pulsarParams.put(TLS_TRUST_CERTS_FILE_PATH, "/path/to/test/tls/cert/cert.pem")
    pulsarParams.put(TLS_ALLOW_INSECURE_CONNECTION, "false")
    pulsarParams.put(TLS_HOSTNAME_VERIFICATION_ENABLE, "false")
    val clientBuilderMock = mock[ClientBuilder]
    when(clientBuilderMock.serviceUrl(any())).thenReturn(clientBuilderMock)
    when(clientBuilderMock.loadConf(any())).thenReturn(clientBuilderMock)
    when(clientBuilderMock.enableTlsHostnameVerification(any())).thenReturn(clientBuilderMock)
    when(clientBuilderMock.allowTlsInsecureConnection(any())).thenReturn(clientBuilderMock)
    when(clientBuilderMock.authentication(any())).thenReturn(clientBuilderMock)

    CachedPulsarClient.createPulsarClient(pulsarParams, clientBuilderMock)

    verify(clientBuilderMock).authentication("unit.test.TestAuthenticationPlugin",
      "token:abc.def.ghi")
    verify(clientBuilderMock).tlsTrustCertsFilePath("/path/to/test/tls/cert/cert.pem")
    verify(clientBuilderMock).allowTlsInsecureConnection(false)
    verify(clientBuilderMock).enableTlsHostnameVerification(false)
    verify(clientBuilderMock).build()
  }

  test("Should build Pulsar client against test Pulsar cluster without exceptions") {
    val pulsarParams = new ju.HashMap[String, Object]()
    pulsarParams.put(AUTH_PLUGIN_CLASS_NAME,
      "org.apache.pulsar.client.impl.auth.AuthenticationToken")
    pulsarParams.put(AUTH_PARAMS, "token:abc.def.ghi")
    pulsarParams.put(SERVICE_URL_OPTION_KEY, "pulsar://127.0.0.1:6650")

    CachedPulsarClient.getOrCreate(pulsarParams)
  }
}