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

import com.google.common.collect.Sets;
import com.google.common.io.MoreFiles;
import com.google.common.io.RecursiveDeleteOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import lombok.extern.slf4j.Slf4j;
import org.apache.bookkeeper.test.PortManager;
import org.apache.pulsar.broker.PulsarService;
import org.apache.pulsar.broker.ServiceConfiguration;
import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.common.naming.TopicName;
import org.apache.pulsar.common.policies.data.ClusterData;
import org.apache.pulsar.common.policies.data.TenantInfo;
import org.apache.pulsar.zookeeper.LocalBookkeeperEnsemble;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

/**
 * A test case to test pulsar service.
 */
@Slf4j
public abstract class PulsarServiceTestCase {

    static Path testDataPath;
    static LocalBookkeeperEnsemble localEnsemble;
    static PulsarService pulsarService;
    static int zkPort;
    static int brokerServicePort;
    static int webServicePort;

    protected PulsarClient client;
    protected PulsarAdmin admin;

    @BeforeClass
    public static void setupCluster() throws Exception {
        testDataPath = Files.createTempDirectory("pulsar");

        Path zkDirPath = Files.createDirectory(Paths.get(testDataPath.toString(), "zk"));
        Path bkDirPath = Files.createDirectory(Paths.get(testDataPath.toString(), "bk"));

        zkPort = PortManager.nextFreePort();

        localEnsemble = new LocalBookkeeperEnsemble(
            3,
            zkPort,
            0,
            zkDirPath.toString(),
            bkDirPath.toString(),
            true,
            "127.0.0.1",
            () -> PortManager.nextFreePort());
        localEnsemble.start();

        brokerServicePort = PortManager.nextFreePort();
        webServicePort = PortManager.nextFreePort();

        ServiceConfiguration serviceConf = new ServiceConfiguration();
        serviceConf.setBrokerServicePort(brokerServicePort);
        serviceConf.setWebServicePort(webServicePort);
        serviceConf.setAdvertisedAddress("127.0.0.1");
        serviceConf.setClusterName("standalone");
        serviceConf.setZookeeperServers("127.0.0.1:" + zkPort);
        serviceConf.setConfigurationStoreServers("127.0.0.1:" + zkPort);
        serviceConf.setRunningStandalone(true);

        pulsarService = new PulsarService(serviceConf, Optional.empty());
        pulsarService.start();

        try (PulsarAdmin admin = PulsarAdmin.builder()
             .serviceHttpUrl(getWebServiceUrl())
             .build()) {
            createDefaultNameSpace(admin, serviceConf, serviceConf.getClusterName());
        }

    }

    private static void createDefaultNameSpace(PulsarAdmin admin,
                                               ServiceConfiguration config,
                                               String cluster) throws Exception {
        ClusterData clusterData = new ClusterData(
            getWebServiceUrl(), null /* serviceUrlTls */,
            getBrokerServiceUrl(), null /* brokerServiceUrlTls */);
        if (!admin.clusters().getClusters().contains(cluster)) {
            admin.clusters().createCluster(cluster, clusterData);
        } else {
            admin.clusters().updateCluster(cluster, clusterData);
        }

        // Create a public tenant and default namespace
        final String publicTenant = TopicName.PUBLIC_TENANT;
        final String defaultNamespace = TopicName.PUBLIC_TENANT + "/" + TopicName.DEFAULT_NAMESPACE;
        if (!admin.tenants().getTenants().contains(publicTenant)) {
            admin.tenants().createTenant(publicTenant,
                    new TenantInfo(Sets.newHashSet(config.getSuperUserRoles()), Sets.newHashSet(cluster)));
        }
        if (!admin.namespaces().getNamespaces(publicTenant).contains(defaultNamespace)) {
            admin.namespaces().createNamespace(defaultNamespace);
            admin.namespaces().setNamespaceReplicationClusters(
                defaultNamespace, Sets.newHashSet(config.getClusterName()));
        }
    }

    @AfterClass
    public static void teardownCluster() throws Exception {
        if (null != pulsarService) {
            pulsarService.close();
        }
        if (null != localEnsemble) {
            localEnsemble.stop();
        }

        MoreFiles.deleteRecursively(testDataPath, RecursiveDeleteOption.ALLOW_INSECURE);
    }

    protected static String getZkServers() {
        return "127.0.0.1:" + zkPort;
    }

    protected static String getBrokerServiceUrl() {
        return "pulsar://127.0.0.1:" + brokerServicePort;
    }

    protected static String getWebServiceUrl() {
        return "http://127.0.0.1:" + webServicePort;
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
