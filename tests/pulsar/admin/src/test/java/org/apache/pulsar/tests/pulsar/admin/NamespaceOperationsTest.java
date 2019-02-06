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
package org.apache.pulsar.tests.pulsar.admin;

import static org.apache.pulsar.common.naming.TopicName.PUBLIC_TENANT;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.admin.PulsarAdminException;
import org.apache.pulsar.common.naming.NamespaceName;
import org.apache.pulsar.tests.common.framework.FrameworkUtils;
import org.apache.pulsar.tests.common.framework.SystemTestRunner;
import org.apache.pulsar.tests.common.framework.SystemTestRunner.TestSuiteClass;
import org.apache.pulsar.tests.pulsar.service.PulsarService;
import org.apache.pulsar.tests.pulsar.suites.PulsarServiceSystemTestCase;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

/**
 * A system test that tests basic namespace operations.
 */
@RunWith(SystemTestRunner.class)
@TestSuiteClass(NamespaceTestSuite.class)
@Slf4j
public class NamespaceOperationsTest extends PulsarServiceSystemTestCase {

    public NamespaceOperationsTest(PulsarService service) {
        super(service);
    }

    private PulsarAdmin admin;

    @Before
    public void setup() throws Exception {
        this.admin = PulsarAdmin.builder()
            .serviceHttpUrl(pulsarService.getHttpServiceUrl())
            .build();
    }

    @After
    public void teardown() throws Exception {
        if (null != this.admin) {
            this.admin.close();
        }
    }

    @Test
    public void testCreateDeleteNamespace() throws Exception {
        final String tenant = PUBLIC_TENANT;

        List<String> namespaces = admin.namespaces().getNamespaces(tenant);

        log.info("Current namespaces of tenant `{}`: {}", tenant, namespaces);

        final String namespace = "test-namespace-" + FrameworkUtils.randomName(16);
        final String fullyQualifiedNs = NamespaceName.get(tenant, namespace).toString();
        // create namespace
        admin.namespaces().createNamespace(fullyQualifiedNs);

        // check if the namespace is created
        namespaces = admin.namespaces().getNamespaces(tenant);
        assertTrue(
            "`" + namespace + "` should be created in tenant `" + tenant + "` but found : " + namespaces,
            namespaces.contains(fullyQualifiedNs));

        // create namespace with the same name will fail
        try {
            admin.namespaces().createNamespace(fullyQualifiedNs);
            fail("Should fail to create a namespace using the same name");
        } catch (PulsarAdminException pae) {
            assertTrue(
                pae.getMessage().contains("Namespace already exists")
            );
        }

        // delete the namespace
        admin.namespaces().deleteNamespace(fullyQualifiedNs);

        // check if the namespace was deleted
        namespaces = admin.namespaces().getNamespaces(tenant);
        assertFalse(
            "`" + namespace + "` should be deleted from tenant `" + tenant + "` but found : " + namespaces,
            namespaces.contains(fullyQualifiedNs)
        );
    }

}
