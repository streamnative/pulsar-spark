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
package org.apache.pulsar.tests.pulsar.suites;

import org.apache.pulsar.tests.common.framework.FrameworkUtils;
import org.apache.pulsar.tests.common.framework.SystemTestSuite.ServiceClass;
import org.apache.pulsar.tests.common.framework.SystemTestSuite.ServiceProvider;
import org.apache.pulsar.tests.pulsar.service.PulsarDeployment;
import org.apache.pulsar.tests.pulsar.service.PulsarService;
import org.apache.pulsar.tests.pulsar.service.PulsarServiceFactory;
import org.apache.pulsar.tests.pulsar.service.PulsarServiceSpec;
import org.junit.AfterClass;
import org.junit.BeforeClass;

/**
 * A test suite that run tests over a pre-deployed pulsar service.
 */
@ServiceClass(PulsarService.class)
public abstract class PulsarServiceTestSuite {

    private static final String PULSAR_DEPLOYMENT_TYPE = "pulsar.deployment.type";

    private static PulsarDeployment pulsarDeployment;
    private static PulsarService pulsarService;

    @BeforeClass
    public static void setupCluster() throws Exception {
        PulsarServiceSpec spec = PulsarServiceSpec.builder()
            .clusterName(FrameworkUtils.randomName(8))
            .build();
        setupCluster(spec);
    }

    private static void setupCluster(PulsarServiceSpec spec) throws Exception {
        pulsarDeployment = PulsarDeployment.valueOf(
            FrameworkUtils.getConfig(PULSAR_DEPLOYMENT_TYPE, PulsarDeployment.TESTCONTAINER_STANDALONE.name())
        );
        pulsarService = PulsarServiceFactory.createPulsarService(
            pulsarDeployment,
            spec
        );
        pulsarService.start();
    }

    @AfterClass
    public static void teardownCluster() {
        if (null != pulsarService) {
            pulsarService.stop();
        }
    }

    @ServiceProvider
    public static PulsarService pulsarService() {
        return pulsarService;
    }

}
