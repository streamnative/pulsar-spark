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
package org.apache.pulsar.segment.test.common;

import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * A test case to test pulsar service.
 */
@Slf4j
public abstract class PulsarServiceTestCase {

    private static final PulsarServiceResource PULSAR_SERVICE_RESOURCE = new PulsarServiceResource();

    protected PulsarClient client;
    protected PulsarAdmin admin;

    @BeforeClass
    public static void setupCluster() throws Throwable {
        PULSAR_SERVICE_RESOURCE.before();
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        PULSAR_SERVICE_RESOURCE.after();
    }

    protected static String getZkServers() {
        return PULSAR_SERVICE_RESOURCE.getZkServers();
    }

    protected static String getBrokerServiceUrl() {
        return PULSAR_SERVICE_RESOURCE.getBrokerServiceUrl();
    }

    protected static String getWebServiceUrl() {
        return PULSAR_SERVICE_RESOURCE.getWebServiceUrl();
    }

    @Before
    public void setup() throws Exception {
        client = PulsarClient.builder()
            .serviceUrl(getBrokerServiceUrl())
            .build();
        log.info("Connect to pulsar broker service : {}", getBrokerServiceUrl());
        admin = PulsarAdmin.builder()
            .serviceHttpUrl(getWebServiceUrl())
            .build();
        log.info("Connect to pulsar web service : {}", getWebServiceUrl());

        doSetup();
    }

    protected void doSetup() throws Exception {}

    @After
    public void teardown() throws Exception {
        doTeardown();

        if (null != admin) {
            admin.close();
        }
        if (null != client) {
            client.close();
        }
    }

    protected void doTeardown() throws Exception {}

}
